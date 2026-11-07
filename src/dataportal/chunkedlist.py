import functools
import logging
import os
import weakref
from urllib.parse import quote

import requests
from frozendict import frozendict

from dataportal.logger import get_logger

_logger = get_logger()

def freezeargs(func):
    """
        Convert a mutable dictionary into immutable. Useful to be compatible with cache.
    """

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        args = (frozendict(arg) if isinstance(arg, dict) else arg for arg in args)
        kwargs = {k: frozendict(v) if isinstance(v, dict) else v for k, v in kwargs.items()}
        return func(*args, **kwargs)

    # functools.wraps does not carry over the cache controls of an lru_cache-wrapped function,
    # so expose them explicitly; otherwise cache_clear() would be hidden by this decorator.
    for name in ("cache_clear", "cache_info"):
        if hasattr(func, name):
            setattr(wrapped, name, getattr(func, name))
    return wrapped

class ChunkedList:
    """
        A class for iterating through items from the API. Behaves like a python iterator, it can be used
        multple times to iterate the remote API. Also supports indexing like a dict on the remote fileId.

        Methods
        -------
        getCount
            get the total number of files with the current filters.
        getBufferCount
            get the number of items in the local buffer from the current filter.

        Cache
        ------
        Requests to the remote server are cached to speed up listing and subsequent calls. Each
        ChunkedList instance has its own cache of at most 8192 calls, which is released together with
        the instance. In special cases when the request cache
        needs to be cleared due to server updates, call instance._sendRequest.cache_clear() to clear
        one instance, or ChunkedList._sendRequest.cache_clear() to clear the caches of all live instances.
    """

    # Weak registry of live instances, used by ChunkedList._sendRequest.cache_clear (see bottom of file).
    _instances = weakref.WeakSet()

    def __init__(self,
                 url: str,
                 path: str,
                 token: str,
                 index: int | str | None = None,
                 limit: int = 2000,
                 sort: str = 'asc',
                 orderBy: str | None = "FileID",
                 startDate: str | None = None,
                 endDate: str | None = None,
                 filter: str | None = None,
                 excludeFilter: str | None = None,
                 extraFiles: bool = False,
                 allowedOrderFields: list | None = None
            ):

        if allowedOrderFields is None:
            allowedOrderFields = [
                "FileID",
                "MFileName",
                "StartDate",
                "StopDate",
                "FileSize",
                "MetricEntries"
            ]

        # Some assertions at creation
        if orderBy not in allowedOrderFields:
            raise ValueError(f"orderBy must be one of {allowedOrderFields}, not '{orderBy}'")
        if sort not in ['asc', 'desc']:
            raise ValueError(f"sort must be one of ['asc', 'desc'], not '{sort}'")

        self._s = requests.Session()

        self._installRequestCache()

        self._url = url
        self._path = path
        self._token = token
        self._orderBy = orderBy
        self._sort = sort
        self._startDate = startDate
        self._endDate = endDate
        self._filter = filter
        self._excludeFilter = excludeFilter
        self._extraFiles = extraFiles

        # Where in local data we are
        self._index = 0
        self._data = None

        # Where in remote data we are
        # currently direction not used in client
        self._limit = limit
        self._firstPageIndex = index
        self._currentPageIndex = index
        self._nextPageIndex = None

        # info about data
        self._totalCount = None

    def _installRequestCache(self):
        """
            Give this instance its own request cache and register it for the class-level clear.

            Per-instance rather than on the class: an lru_cache on a method lives on the CLASS, which
            is a GC root, so every instance that ever made a request was retained forever together
            with its buffered _data. Owning the cache makes it die with the instance.

            Note what this did NOT fix: the cache key already included `self`, so two instances never
            shared entries -- measured, B always fetched its own data. Staleness within ONE instance
            (the docstring's "could lead to new data not beeing fetched") is unchanged and intended.

            functools.partial rather than a bound method: lru_cache copies the wrapped object's
            attributes, and a bound method exposes the class-level function's ones -- including the
            class-wide cache_clear attached at the bottom of this file -- which would shadow this
            instance's own cache_clear and recurse.
        """
        self._sendRequest = freezeargs(functools.lru_cache(maxsize=8*1024)(functools.partial(type(self)._sendRequest, self)))
        ChunkedList._instances.add(self)

    # The per-instance cache above is a live lru_cache wrapper, which pickle cannot serialise, so it is
    # dropped on the way out and rebuilt on the way in. Without this, pickling a ChunkedList raises
    # PicklingError -- it worked before the cache moved onto the instance, so leaving it broken would be
    # a silent regression. The rebuilt instance starts with an empty cache, which is the safe direction.
    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop("_sendRequest", None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._installRequestCache()

    def getCount(self):
        """
            Returns the total amount of files.
        """
        if self._totalCount is None:
            self._fetch_data(self._firstPageIndex)
        return self._totalCount

    def getBufferCount(self):
        """
            Returns the number of files in the buffer. Should be limit, or lower if on last page.
        """
        if self._totalCount is None:
            self._fetch_data(self._firstPageIndex)
        return len(self._data)

    # JR: (old 10*1024*1024) according to https://docs.python.org/3/library/functools.html, maxsize seems to be
    # number of function calls to save, not max size in bytes
    # This is the uncached implementation; __init__ replaces it on each instance with a per-instance
    # freezeargs(lru_cache(...)) wrapper around the bound method.
    def _sendRequest(self, path: str, query: dict | None = None, stream: bool = False):
        """
            Sends the requests to the remote server. On an instance this is a per-instance lru cache
            (set up in __init__) to speed subsequent requests to the server (this could lead to new data
            not beeing fetched). The cache saves at most 8192 function calls, is private to the instance,
            is freed together with it, and can be reset with instance._sendRequest.cache_clear().
            ChunkedList._sendRequest.cache_clear() clears the caches of all live instances.
        """
        if query is None:
            query = {}
        if query:
            queryString = "?" + "&".join([f"{key}={quote(str(value))}" for (key, value) in query.items() if value is not None])
        else:
            queryString = ""
        # _logger.info(f"sending {path + queryString}")
        _logger.info("sending %s%s", os.path.join(self._url, path), queryString)  # noqa: PTH118 - Path would eat the // in the URL scheme
        response = self._s.get(os.path.join(self._url, path) + queryString, headers={"Authorization": "Bearer " + self._token}, stream=stream)  # noqa: PTH118 - Path would eat the // in the URL scheme
        response.raise_for_status()
        return response

    def _fetch_data(self, index: int | str | None):
        """
            Fetches data and caches the data locally in the object.
        """

        _logger.debug(f"fetch data index={index} "
                      f"limit={self._limit} "
                      f"order_by={self._orderBy} "
                      f"sort={self._sort} "
                      f"start_date={self._startDate} "
                      f"end_date={self._endDate} "
                      f"filter={self._filter} "
                      f"excludeFilter={self._excludeFilter} "
                      f"extraFiles={self._extraFiles}")

        data = self._sendRequest(f"{self._path}",
                    query={
                        "index": index,
                        "limit": self._limit,
                        "order_by": self._orderBy,
                        "sort_direction": self._sort,
                        "start_date": self._startDate,
                        "end_date": self._endDate,
                        "filter": self._filter,
                        "excludeFilter": self._excludeFilter,
                        "extrafiles": 'true' if self._extraFiles else 'false'
                    }
                ).json()
        _logger.log(logging.VERBOSE, data)

        self._totalCount = data['pagination']['total_records']

        # next_page empty dict if no next page, default "default value" for .get() is None ;)
        self._nextPageIndex = data['pagination']['next_page'].get('index')

        # current_page always has index, except when index not given in API call while there are no files in dataset
        self._currentPageIndex = data['pagination']['current_page'].get('index')

        self._data = data['data']

    def __iter__(self):
        """
            Iterator implementation; this starts the iteration.
        """
        if self._currentPageIndex != self._firstPageIndex or self._data is None:
            _logger.debug("__iter__: fetch")
            #self._sindex = self._save_index
            self._fetch_data(self._firstPageIndex)
        self._index = 0
        return self

    def __next__(self):
        """
            Iterator implementation; called everytime we want an object from the iterator.
        """

        # If no data has been fetched, fetch the first page index
        if self._data is None:
            self._fetch_data(self._firstPageIndex)

        _logger.debug(f"self._index = {self._index}, "
                      f"self._currentPageIndex = {self._currentPageIndex}, "
                      f"self._nextPageIndex = {self._nextPageIndex}, "
                      f"self._limit = {self._limit}")

        _logger.debug((not self._data) or ((len(self._data) < self._limit) and self._index >= len(self._data)))
        _logger.debug(f"(not {bool(self._data)}) or (({len(self._data)} < {self._limit}) and {self._index} >= {len(self._data)})")

        # If no data has been set, or there are no datafiles, stop iteration directly
        if (not self._data) or (len(self._data) == 0):
            raise StopIteration

        _logger.debug("index: %i, len(data): %i", self._index, len(self._data))
        # if out of range fetch new make this in the background on last entry
        if self._index >= len(self._data):
            _logger.debug("fetch more")
            # Stop if we have passed the final index and there are no more chunks
            if self._nextPageIndex is not None:
                self._fetch_data(self._nextPageIndex)
            else:
                raise StopIteration
            self._index = 0

        data = self._data[self._index]
        self._index += 1

        _logger.debug("index: %i, data[index]: %s", self._index, data)
        return data

    # behave as dict
    def __getitem__(self, index: int):
        """
            Overrride getitem so we can behave as a dict.

            Returns the record for `index` whether it is a datafile or an extrafile, raises
            KeyError when no such FileID exists, and propagates anything else.

            Parameters
            ----------
            index : int
                index is the remote id, not the index in the file list.
        """
        # This used to retry a 404 as .../{index}?extrafile=true, on the assumption that
        # extrafiles needed a separate lookup. Measured against the portal (v1.3.2): the plain
        # route already returns extrafiles, and that route accepts NO extrafile parameter --
        # both `extrafile` and `extrafiles` answer 400 "Unknown query parameter". So the retry
        # could only ever turn a 404 into a 400, which the old code then swallowed, returning
        # None for a missing FileID; the KeyError below was unreachable, which is also why the
        # message sat there with an unformatted "{index}" in it. One request is correct.
        try:
            return self._sendRequest(f"{self._path}/{index}").json()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                raise KeyError(f"FileID {index} does not exist") from e
            raise

    def __len__(self):
        """
            Override len to make it behave as a list/iterator.
        """
        return self.getCount()


# Registry of live instances (weak, so it never keeps one alive). Caches are per instance, but the
# documented public API ChunkedList._sendRequest.cache_clear() predates that and must keep working;
# it is attached to the class-level function and clears the cache of every live instance.
def _clear_all_caches():
    for instance in list(ChunkedList._instances):
        instance._sendRequest.cache_clear()


ChunkedList._sendRequest.cache_clear = _clear_all_caches
