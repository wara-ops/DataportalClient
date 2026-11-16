"""
Turns the portal's authentication / authorisation failures into messages a notebook user can act on.

Without this a rejected request surfaces as requests' own
``HTTPError: 401 Client Error: Unauthorized for url: http://<in-cluster service name>/v1/...``, which
says nothing about the cause and shows an internal Kubernetes DNS name.

The portal answers 401 for three unrelated reasons (an unusable token, a revoked token, and a valid token
whose user has no access to the dataset), so the status code alone cannot be used: the response BODY is
what tells them apart. The bodies matched below are the portal's own (wcib ``middleware/jwt.js``,
``middleware/session.js`` and ``middleware/apiScopeValidator.js``); they are not verified against a live
portal from this repository, see the tests for the exact strings assumed.

SECURITY -- reading the token's claims is NOT trusting them.
    The token is a JWS (signed, not encrypted), so its payload is readable here with the standard library
    alone. From the client's point of view that payload is UNSIGNED DATA: the client does not hold the
    signing key, and the user can edit it freely. Therefore nothing in this module may use a claim to decide
    what the user is allowed to do, or to choose a code path that grants anything. In particular ``groups``,
    ``organization``, ``apiScope`` and ``permissions`` are tempting to act on and must NOT be. Claims are used
    only for two things, both of which are safe to get wrong:

      1. Explaining a failure the SERVER has already returned (who you are, what the token is scoped to).
         A forged claim can then at worst make the explanation inaccurate.
      2. The pre-flight expiry check on connect (``exp``). A forged ``exp`` can at worst make the client
         refuse to connect, or skip the check and let the server answer, with a confusing message. It never
         grants access: the server verifies the signature and expiry on every request regardless.
"""

import base64
import binascii
import json
import math
import re
import time
from datetime import UTC, datetime
from urllib.parse import urlsplit

import requests

__all__ = [
    "DataportalAuthError",
    "DatasetAccessDeniedError",
    "TokenExpiredError",
    "TokenRejectedError",
    "TokenRevokedError",
    "TokenScopeError",
    "checkTokenExpiry",
    "decodeTokenClaims",
    "expiryWarnWindow",
    "expiryWarning",
    "explainHTTPError",
    "raiseForStatus",
    "redactUrls",
    "sanitiseError",
]


class DataportalAuthError(requests.exceptions.HTTPError):
    """
        Base class for authentication and authorisation failures against the dataportal.

        It subclasses requests' HTTPError on purpose: code that already catches HTTPError (including this
        package's own getExtraFiles) keeps catching these. ``.response`` is the portal's response when the
        failure came from one, and None for the pre-flight expiry check, which makes no request. The original
        error is available as ``__cause__``.
    """


class TokenExpiredError(DataportalAuthError):
    """The token's expiry time has passed."""


class TokenRevokedError(DataportalAuthError):
    """The token was revoked (blacklisted) on the portal."""


class TokenRejectedError(DataportalAuthError):
    """The portal refused the token, and it is not expired: wrong environment, truncated or altered."""


class DatasetAccessDeniedError(DataportalAuthError):
    """The token authenticated, but the user is not allowed to do this on the dataset."""


class TokenScopeError(DataportalAuthError):
    """The token is restricted (scoped) and the request falls outside what it allows."""


# --- reading the token ------------------------------------------------------------------------------------------

def decodeTokenClaims(token: str) -> dict:
    """
        Returns the payload of a JWT as a dict, WITHOUT verifying the signature, or {} if the token is not a
        JWT (users may hold other token shapes) or its payload is not a JSON object.

        NOT trusted -- see the module docstring. Only for explaining a failure the server already reported
        and for the pre-flight expiry check.
    """
    try:
        parts = token.strip().split(".")
        if len(parts) != 3:
            return {}
        payload = parts[1]
        # JWTs use URL-safe base64 with the '=' padding stripped; both need handling.
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    except (ValueError, TypeError, AttributeError, binascii.Error):
        # ValueError covers bad base64, non-UTF-8 bytes and invalid JSON (JSONDecodeError, UnicodeDecodeError).
        return {}
    return claims if isinstance(claims, dict) else {}


def _expiry(claims: dict) -> float | None:
    """The ``exp`` claim as epoch seconds, or None if absent or not a usable number."""
    exp = claims.get("exp")
    if isinstance(exp, bool) or not isinstance(exp, int | float):
        return None
    try:
        value = float(exp)  # OverflowError for an int too large for a float
    except OverflowError:
        return None
    return value if math.isfinite(value) else None


def _formatInstant(epoch: float) -> str:
    try:
        return datetime.fromtimestamp(epoch, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (OverflowError, OSError, ValueError):
        return f"{epoch:.0f} (epoch seconds)"


def _formatAgo(seconds: float) -> str:
    if seconds < 90:
        return f"{max(seconds, 0):.0f} seconds"
    if seconds < 90 * 60:
        return f"{seconds / 60:.1f} minutes"
    if seconds < 48 * 3600:
        return f"{seconds / 3600:.1f} hours"
    return f"{seconds / 86400:.1f} days"


def _expiredMessage(exp: float, now: float) -> str:
    return (
        f"Your dataportal token expired at {_formatInstant(exp)} ({_formatAgo(now - exp)} ago, by this machine's clock). "
        "Get a new token from the portal and create the client with it."
    )


# When to warn that a token is about to expire. Token lifetimes here vary by orders of magnitude (about 20 minutes for
# a notebook token, 1095 days for an MQTT setup token), so one flat duration is wrong for most of them: "warn under
# 1 hour" cries wolf on every connect with a 20-minute token and never means anything for a 3-year one. The window is
# therefore the token's own lifetime scaled, bounded on both sides:
#
#   window = clamp(EXPIRY_WARN_FRACTION * (exp - iat), EXPIRY_WARN_FLOOR_S, EXPIRY_WARN_CEILING_S)
#
#   floor    5 minutes. Whatever the lifetime, a download that is shorter than this should still be able to finish
#            and the user should have time to react. Alone it decides for a token with no `iat` (no lifetime known).
#            For a 20-minute token it gives a window of 5 minutes (the last quarter), not the 2 minutes 10% would.
#   fraction 10% of the lifetime. A 1-hour token warns in its last 6 minutes, a 24-hour one in its last 2.4 hours.
#   ceiling  1 day. Without it the 1095-day token would warn for its last 109 days, i.e. on every connect for months,
#            which is noise; no single operation in a notebook needs more than a day of validity.
#
# All three are only defaults: DataportalClient(warnBeforeExpiry=seconds) replaces the window with that flat number
# (0 turns the warning off).
EXPIRY_WARN_FLOOR_S = 5 * 60
EXPIRY_WARN_FRACTION = 0.10
EXPIRY_WARN_CEILING_S = 24 * 3600


def expiryWarnWindow(claims: dict, override: float | None = None) -> float:
    """Seconds before ``exp`` within which a still-valid token is reported as nearly expired. See the constants above."""
    if override is not None:
        return max(float(override), 0.0)
    window = EXPIRY_WARN_FLOOR_S
    exp, iat = _expiry(claims), claims.get("iat")
    iat = _expiry({"exp": iat})  # same "usable number" rule as exp
    if exp is not None and iat is not None and exp > iat:
        window = max(window, EXPIRY_WARN_FRACTION * (exp - iat))
    return min(window, max(EXPIRY_WARN_CEILING_S, EXPIRY_WARN_FLOOR_S))


def expiryWarning(token: str, warnBefore: float | None = None, now: float | None = None) -> str | None:
    """
        Returns a warning text if the token is still valid by its ``exp`` claim but within the warning window of
        expiring, otherwise None (comfortably valid, expired, not a JWT, no usable ``exp``). Never raises.

        Used both at connect and before a streaming download. An unverified claim only decides whether a log line is
        written; it never stops a request, so a forged ``exp`` can at worst produce or hide a warning.
    """
    claims = decodeTokenClaims(token)
    exp = _expiry(claims)
    now = time.time() if now is None else now
    if exp is None or exp <= now:
        return None
    remaining = exp - now
    if remaining >= expiryWarnWindow(claims, warnBefore):
        return None
    return (
        f"dataportal token expires at {_formatInstant(exp)} (in {_formatAgo(remaining)}); "
        "long downloads may fail part-way - consider a fresh token"
    )


def checkTokenExpiry(token: str, warnBefore: float | None = None, now: float | None = None) -> str | None:
    """
        Pre-flight check run before connecting. Raises TokenExpiredError if the token's ``exp`` is already in the
        past, so the user is told so without a network round trip. Otherwise returns expiryWarning() (a text for the
        caller to log, or None). A token that is not a JWT, or has no usable ``exp``, passes silently and the server
        decides.

        Safe to act on an unverified claim here only because the worst case of a forged ``exp`` is a refusal to
        connect or a skipped check; the server enforces expiry itself (see the module docstring).
    """
    exp = _expiry(decodeTokenClaims(token))
    now = time.time() if now is None else now
    if exp is not None and exp <= now:
        raise TokenExpiredError(_expiredMessage(exp, now))
    return expiryWarning(token, warnBefore, now)


# --- explaining a server refusal --------------------------------------------------------------------------------

_BODY_USER_RE = re.compile(r"user (.+?) has no permissions", re.IGNORECASE)
_BODY_DATASET_RE = re.compile(r"dataset(?: with ID)? (\d+)", re.IGNORECASE)
_PATH_DATASET_RE = re.compile(r"(?:^|/)dataset/(\d+)")


def _bodyOf(response) -> str:
    try:
        return (response.text or "").strip()
    except Exception:  # noqa: BLE001 - a body that cannot be read just means we explain from the status alone
        return ""


def _describeScope(claims: dict) -> str:
    """What the token claims to be scoped to, for the explanation only."""
    parts = []
    api = claims.get("apiScope")
    if isinstance(api, list):
        entries = []
        for scope in api:
            if isinstance(scope, dict) and "path" in scope:
                entries.append(f"{scope['path']} ({scope.get('acc', '?')})")
        parts.append(", ".join(entries) if entries else "nothing (an empty scope allows no API access)")
    permissions = claims.get("permissions")
    if isinstance(permissions, dict) and permissions:
        parts.append(f"dataset {permissions.get('dataset', '?')}, actions {permissions.get('actions', [])}")
    return "; ".join(parts)


def explainHTTPError(
    err: requests.exceptions.HTTPError,
    token: str,
    path: str,
    datasetName: str | None = None,
    datasetId: int | None = None,
    now: float | None = None,
) -> DataportalAuthError | None:
    """
        Returns the clear exception for an HTTPError that is an authentication or authorisation failure, or None
        if it is anything else (404, 500, a 403 this module does not recognise, a response-less error) and
        should be raised as it is.

        path is the API path that was requested (used to say what fell outside a scoped token). datasetName and
        datasetId are the dataset the client was working with; the id in the server's own response wins over
        them, and the name is only used when it belongs to that id.

        The claims read from token are only used to word the message, never to decide the outcome; the outcome
        comes from the status code and body the server sent. (Expiry is the one exception: when the server
        said 401 with no more specific reason, the local ``exp`` picks between the "expired" and "rejected"
        wording. It cannot change that the request failed.)
    """
    response = err.response
    status = getattr(response, "status_code", None)
    if status not in (401, 403):
        return None

    body = _bodyOf(response)
    low = body.lower()
    claims = decodeTokenClaims(token)
    now = time.time() if now is None else now
    requested = "/" + path.lstrip("/")

    if status == 401 and "has no permissions" in low:
        # Authentication was fine; this user has no role on that dataset. Prefer the username the server named
        # over the (unverified) claim.
        match = _BODY_USER_RE.search(body)
        user = claims.get("user") if isinstance(claims.get("user"), dict) else {}
        username = match.group(1) if match else user.get("username")
        who = f"user '{username}'" if username else "your user"
        found = _BODY_DATASET_RE.search(body) or _PATH_DATASET_RE.search(path)
        foundId = int(found.group(1)) if found else datasetId
        if foundId is not None and foundId == datasetId and datasetName:
            dataset = f"'{datasetName}' (ID {foundId})"
        elif foundId is not None:
            dataset = f"with ID {foundId}"
        else:
            dataset = "you were accessing"
        return DatasetAccessDeniedError(
            f"Your token is valid, but {who} has no permission to do this on the dataset {dataset}. "
            "Ask the owner of the dataset to give you access.",
            response=response,
        )

    if status == 401 and "blacklisted" in low:
        return TokenRevokedError(
            "Your dataportal token has been revoked and can no longer be used. Get a new token from the portal.",
            response=response,
        )

    if "token does not have permission" in low:
        scope = _describeScope(claims)
        return TokenScopeError(
            f"This request is outside what your token is allowed to do: GET {requested}. "
            + (f"The token is restricted to: {scope}. " if scope else "The token is restricted (scoped). ")
            + "Get a token whose scope covers this dataset, or an unscoped token.",
            response=response,
        )

    if status == 403 and "current role does not allow" in low:
        return DatasetAccessDeniedError(
            "Your token is valid, but your current role no longer allows this action on the dataset "
            "(your access may have been reduced after the token was issued). Ask the owner of the dataset about your access.",
            response=response,
        )

    if status == 401:
        exp = _expiry(claims)
        if exp is not None and exp <= now:
            return TokenExpiredError(_expiredMessage(exp, now), response=response)
        return TokenRejectedError(
            "The portal rejected your token, but it has not expired. The usual causes are a token from a different "
            "environment (for example a staging token used against production, which has a different signing key), "
            "or a token that was truncated or altered when copied. Get a new token from the portal this notebook connects to.",
            response=response,
        )

    return None


# --- keeping the portal's host out of what the user reads ---------------------------------------------------------

_URL_RE = re.compile(r"""[A-Za-z][A-Za-z0-9+.\-]*://[^\s'"<>]+""")
_URL_TRAILING_PUNCTUATION = ".,;:!?)]}"


def _redactOne(match: re.Match) -> str:
    url = match.group(0)
    trailing = ""
    while url and url[-1] in _URL_TRAILING_PUNCTUATION:  # sentence punctuation after a URL is not part of it
        url, trailing = url[:-1], url[-1] + trailing
    try:
        parts = urlsplit(url)
    except ValueError:  # e.g. an unbalanced '[' in the host: better to hide it than to risk showing the host
        return "<url>" + trailing
    if not parts.netloc:
        return match.group(0)  # no host to hide (e.g. file:///x)
    return (parts.path or "/") + (f"?{parts.query}" if parts.query else "") + trailing


def redactUrls(text: str) -> str:
    """
        Reduces every URL in text to its path and query string: the scheme, host, port and any credentials are dropped,
        the path (which says which call failed) and the query (which says with what) are kept.

            401 Client Error: Unauthorized for url: http://dataportal-wcibapi-service.frontend.svc.cluster.local:3000/v1/validatetoken
            -> 401 Client Error: Unauthorized for url: /v1/validatetoken

        Not tied to one hostname: whatever DATAPORTAL_API points at (localhost, an ingress, an IP with a port, the
        in-cluster service name) is removed the same way. A URL is anything of the form scheme://..., wherever it
        occurs in the text.
    """
    return _URL_RE.sub(_redactOne, text)


def sanitiseError(err: BaseException) -> BaseException:
    """
        Rewrites the message of err in place so that it no longer contains a host, and returns the same object.

        Deliberately NOT a copy and NOT suppression: the exception keeps its identity, ``__traceback__``,
        ``__cause__``/``__context__`` and attributes (``.response``, ``.request``), so callers that read
        ``e.response.status_code`` and anyone inspecting the chain see exactly what requests raised. Only the rendered
        message (``str(err)``, which is ``args``) changes. Only string arguments are rewritten.
    """
    err.args = tuple(redactUrls(a) if isinstance(a, str) else a for a in err.args)
    return err


def raiseForStatus(response, token: str, path: str, datasetName: str | None = None, datasetId: int | None = None) -> None:
    """
        Like ``response.raise_for_status()``, with two differences. This is the single place where this package turns
        an HTTP status into an exception, so both live here and no request path can bypass them.

        1. The message of the HTTPError requests raises is sanitised (see sanitiseError): the host and port are
           removed, the path and query are kept. This applies to EVERY HTTP error, translated or not.
        2. An authentication/authorisation failure is raised as the matching DataportalAuthError, chained to that
           (sanitised, otherwise untouched) HTTPError with ``from err``. Any other failure is re-raised as the very
           same HTTPError object.

        The response is closed when it is translated: with stream=True the caller never gets the response, so it
        would otherwise hold its connection until garbage collected. (This runs before the caller's ``with`` block
        is entered, so that block cannot do it.)
    """
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        sanitiseError(err)
        translated = explainHTTPError(err, token, path, datasetName, datasetId)
        if translated is None:
            raise
        response.close()
        raise translated from err
