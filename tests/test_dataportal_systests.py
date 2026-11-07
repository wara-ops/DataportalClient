import datetime
import hashlib
import io
import json
import logging
import os
import re
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pytest
import requests
import zstandard
from requests.exceptions import HTTPError

from src.dataportal.dataportalclient import ChunkedList, DataportalClient, Dataset, NoDatasetError, UnsupportedExtentionError
from tests.settings import (
    check_cache,
    check_extra_files,
    listdir_names,
    mockdownloaddir,
    systests_datasetname,
    systests_parquet_prefix,
    systests_parquet_rows,
    systests_parquet_stale_age,
    systests_testpw,
    systests_testuser,
    systests_url,
    verify_files,
)

if "DATAPORTAL_API" not in os.environ:
    os.environ["DATAPORTAL_API"] = systests_url
else:
    print(f"DATAPORTAL_API env variable already set to {os.environ['DATAPORTAL_API']}")


_logger = logging.getLogger(__name__)

s = requests.Session()
def session_post(path, json, headers=None):
    if headers is None:
        headers = {}
    response = s.post(os.path.join(os.environ["DATAPORTAL_API"], path), json=json, headers=headers)  # noqa: PTH118 - Path would eat the // in the URL scheme
    response.raise_for_status()
    return response

def session_get(path, headers=None):
    if headers is None:
        headers = {}
    response = s.get(os.path.join(os.environ["DATAPORTAL_API"], path), headers=headers)  # noqa: PTH118 - Path would eat the // in the URL scheme
    response.raise_for_status()
    return response

# test connection
session_get('v1/test')

# login
session_post('v1/login', {'username': systests_testuser, 'password': systests_testpw})

# get token. POST since wcib dab6c25 ("change token to POST not GET", in
# v1.3.1); a GET now 404s. Empty body = default name/expiry/scopes.
token = session_post('v1/generatetoken', {}).json()['access_token']


# Load the dataset, files and extrafiles from the test system matching the testdataset name
datasets = session_get('v1/dataset').json()['Datasets']

try:
    testdataset = [ds for ds in datasets if ds['DatasetName'] == systests_datasetname].pop()
except IndexError as e:
    raise ValueError(f"Cannot find testdataset {systests_datasetname} in test system") from e

response = session_get(f"v1/dataset/{testdataset['DatasetID']}/files").json()

testfiles = response['data']
pagination = response['pagination']

assert pagination['count'] == pagination['total_records'], f"testfiles must include all files in the dataset, but now count = {pagination['count']} while total_records = {pagination['total_records']}"
assert len(testfiles) >= 10, f"testdataset {systests_datasetname} needs at least 10 files to function, found {len(testfiles)}"

response = session_get(f"v1/dataset/{testdataset['DatasetID']}/files?extrafiles=true").json()
testextrafiles = response['data']
assert len(testextrafiles) >= 0, f"testdataset {systests_datasetname} needs extrafiles, found none"

# datetime in python 3.10 does not recognize Z as alias for +00:00
def fromisoformat_fix(s: str) -> datetime.datetime:
    try:
        return datetime.datetime.fromisoformat(s)
    except ValueError:
        return datetime.datetime.fromisoformat(s.strip('Z'))


@pytest.fixture
def client_clean():
    return DataportalClient(token, cleanStart=True)

@pytest.fixture(scope='module')
def client_testds():
    return DataportalClient(token, cleanStart=True).fromDataset(testdataset['DatasetName'])

class TestCreateClientFromTestSystem:
    def test_create_client(self, caplog):
        with caplog.at_level(logging.INFO):
            DataportalClient(token)
            assert "Connection OK" in caplog.text

    def test_create_clean_client(self, caplog):
        with caplog.at_level(logging.INFO):
            DataportalClient(token, cleanStart=True)
            assert "Connection OK" in caplog.text

    def test_wrong_token(self):
        with pytest.raises(HTTPError):
            DataportalClient('badtoken')

class TestLoadDatasetFromTestSystem:
    def test_load_dataset_with_name(self, client_clean: DataportalClient):
        print(datasets)
        client_loaded = client_clean.fromDataset(testdataset['DatasetName'])
        assert isinstance(client_loaded, DataportalClient)
        assert client_loaded == client_clean
        assert client_loaded.state.dataset.DatasetName == testdataset['DatasetName']
        assert client_loaded.state.dataset.DatasetID == testdataset['DatasetID']

    def test_load_dataset_with_ID(self, client_clean: DataportalClient):
        client_loaded = client_clean.fromDataset(testdataset['DatasetID'])
        assert isinstance(client_loaded, DataportalClient)
        assert client_loaded == client_clean
        assert client_loaded.state.dataset.DatasetName == testdataset['DatasetName']
        assert client_loaded.state.dataset.DatasetID == testdataset['DatasetID']

    def test_get_current_dataset(self, client_testds: DataportalClient, capsys):
        ds = client_testds.currentDataset()

        to_match = {}
        for (field, info) in Dataset.model_fields.items():
            if info.annotation == datetime.datetime:
                to_match[field] = fromisoformat_fix(testdataset[field]).strftime('%Y-%m-%dT%H:%M:%SZ')
            else:
                to_match[field] = testdataset[field]

        assert json.loads(ds.model_dump_json()) == to_match

    def test_nonexistent_dataset(self, client_clean: DataportalClient):
        with pytest.raises(ValueError):
            client_clean.fromDataset('doesnotexist')

class TestListFilesFromTestSystem:

    def test_no_loaded_dataset(self, client_clean: DataportalClient):
        with pytest.raises(NoDatasetError):
            client_clean.listFiles()

    def test_list_files(self, client_testds: DataportalClient, capsys):
        files = client_testds.listFiles()
        captured = capsys.readouterr()

        assert isinstance(files, ChunkedList)
        assert len(files) == len(testfiles)
        assert captured.out == ''

    def test_list_files_iterator(self, client_testds: DataportalClient):
        files = client_testds.listFiles()

        assert isinstance(files, ChunkedList)

        # Test that we can manually step forward and get all files
        for testf in testfiles:
            assert next(files) == testf

        # Test that we properly get a stop iteration at end
        with pytest.raises(StopIteration):
            next(files)

        # Test that we can reset iterator and get all files with list/for-loop
        assert list(files) == testfiles

        files_list = []
        for f in files:
            files_list.append(f)
        assert files_list == testfiles

    def test_list_files_index(self, client_testds: DataportalClient):
        files = client_testds.listFiles(index=testfiles[2]['FileID'])
        assert isinstance(files, ChunkedList)
        assert list(files) == testfiles[2:]
        assert testfiles[0]['FileID'] not in [ f['FileID'] for f in files ]
        assert testfiles[1]['FileID'] not in [ f['FileID'] for f in files ]

    def test_list_files_index_limit(self, client_testds: DataportalClient):
        files = client_testds.listFiles(limit=1)
        assert isinstance(files, ChunkedList)
        assert len(files) == len(testfiles)
        assert files.getBufferCount() == 1
        # limit only dictates how many files to save in the local data, we can still use the chunkedlist
        # iterator to step through the files and automatically fetch new data when limit is reached
        assert list(files) == testfiles

        files = client_testds.listFiles(limit=3)
        assert isinstance(files, ChunkedList)
        assert len(files) == len(testfiles)
        assert files.getBufferCount() == 3
        assert list(files) == testfiles

        files = client_testds.listFiles(limit=100)
        assert isinstance(files, ChunkedList)
        assert len(files) == len(testfiles)
        assert files.getBufferCount() == len(testfiles)
        assert list(files) == testfiles

    def test_list_files_filter(self, client_testds: DataportalClient):

        files = client_testds.listFiles(filter="%_2022%.pkl")

        (testfiles_match, _) = verify_files(testfiles, "MFileName", [r".*_2022.*\.pkl"])
        (match, no_match) = verify_files(list(files), "MFileName", [r".*_2022.*\.pkl"])

        assert isinstance(files, ChunkedList)
        assert list(files) == [m[1] for m in testfiles_match]
        assert len(match) == len(files)
        assert len(no_match) == 0


    def test_list_files_exclude_filter(self, client_testds: DataportalClient):
        files = client_testds.listFiles(excludeFilter="%_2022%.pkl")

        (_, testfiles_no_match) = verify_files(testfiles, "MFileName", [r".*_2022.*\.pkl"])
        (match, no_match) = verify_files(list(files), "MFileName", [r".*_2022.*\.pkl"])

        assert isinstance(files, ChunkedList)
        assert list(files) == [m[1] for m in testfiles_no_match]
        assert len(no_match) == len(files)
        assert len(match) == 0

    def test_list_files_sort_direction(self, client_testds: DataportalClient):
        files = client_testds.listFiles(sort='desc')
        assert isinstance(files, ChunkedList)
        assert list(files) == testfiles[::-1]

    def test_list_files_order_by(self, client_testds: DataportalClient):
        testfiles_entriessorted = sorted(testfiles, key = lambda f: f['MetricEntries'])
        files = client_testds.listFiles(orderBy='MetricEntries')

        assert isinstance(files, ChunkedList)
        assert list(files) == testfiles_entriessorted

    # --- ChunkedList indexing against the REAL portal -------------------------
    # files[fileid] had no systest coverage at all, which is how three defects in
    # __getitem__ survived: a KeyError message that printed the literal "{index}",
    # a non-404 response falling off the end of the function and returning None,
    # and the extrafile fallback. The unit tests for these are mocked; these run
    # the same paths against a live portal, where the 404 that triggers the
    # fallback is produced by the portal itself rather than by a fixture.

    def test_getitem_returns_the_datafile(self, client_testds: DataportalClient):
        files = client_testds.listFiles()
        wanted = testfiles[0]

        record = files[wanted['FileID']]

        assert record is not None, "__getitem__ must never return None"
        assert record['FileID'] == wanted['FileID']
        assert record['MFileName'] == wanted['MFileName']

    @pytest.mark.skipif(not testextrafiles, reason="the test dataset has no extrafiles")
    def test_getitem_returns_an_extrafile_from_the_same_route(self, client_testds: DataportalClient):
        # Measured against the portal: the plain single-file route answers 200 for an
        # extrafile, and rejects ?extrafile=true with 400. So no fallback is needed, and
        # a second request would be the dead fallback coming back.
        files = client_testds.listFiles()
        wanted = testextrafiles[0]

        record = files[wanted['FileID']]

        assert record is not None, "__getitem__ must never return None"
        assert record['FileID'] == wanted['FileID']
        assert record['MFileName'] == wanted['MFileName']

    def test_getitem_unknown_id_raises_keyerror_naming_the_id(self, client_testds: DataportalClient):
        files = client_testds.listFiles()
        missing = max(f['FileID'] for f in testfiles + testextrafiles) + 10_000

        with pytest.raises(KeyError) as excinfo:
            files[missing]

        assert str(missing) in str(excinfo.value), "the id must be interpolated, not printed literally"
        assert "{index}" not in str(excinfo.value)

    def test_chunkedlist_default_constructor_against_the_portal(self):
        # ChunkedList(url, path, token) used to raise ValueError from its own validation,
        # because orderBy defaulted to None while None was not an allowed order field.
        chunked = ChunkedList(
            url=os.environ["DATAPORTAL_API"],
            path=f"v1/dataset/{testdataset['DatasetID']}/files",
            token=token,
        )

        listed = list(chunked)

        assert [f['FileID'] for f in listed] == sorted(f['FileID'] for f in testfiles), \
            "the default order must be FileID ascending, matching listFiles()"

    def test_list_files_order_by_with_index(self, client_testds: DataportalClient):
        testfiles_datesorted = sorted(testfiles, key= lambda f: fromisoformat_fix(f['StartDate']))

        files = client_testds.listFiles(orderBy='StartDate', index=testfiles_datesorted[3]['StartDate'])

        assert isinstance(files, ChunkedList)
        assert list(files) == testfiles_datesorted[3:]

    def test_list_files_order_by_with_index_and_sort(self, client_testds: DataportalClient):
        testfiles_datesorted = sorted(testfiles, key= lambda f: fromisoformat_fix(f['StartDate']))

        files = client_testds.listFiles(orderBy='StartDate', sort='desc', index=testfiles_datesorted[3]['StartDate'])

        assert isinstance(files, ChunkedList)
        assert list(files) == testfiles_datesorted[:4][::-1] # Sort/OrderBy is done first

    def test_list_files_date_limits(self, client_testds: DataportalClient):
        testfiles_datesorted = sorted(testfiles, key= lambda f: fromisoformat_fix(f['StartDate']))

        testfiles_to_match = testfiles_datesorted[1:5]
        files = client_testds.listFiles(startDate=testfiles_to_match[0]['StartDate'], endDate=testfiles_to_match[-1]['StopDate'], orderBy='StartDate')
        assert isinstance(files, ChunkedList)
        assert list(files) == testfiles_to_match

        testfiles_to_match = testfiles_datesorted[3:]
        files = client_testds.listFiles(startDate=testfiles_to_match[0]['StartDate'], orderBy="StartDate")
        assert isinstance(files, ChunkedList)
        assert list(files) == testfiles_to_match

        testfiles_to_match = testfiles_datesorted[:7]
        files = client_testds.listFiles(endDate=testfiles_to_match[-1]['StopDate'], orderBy="StartDate")
        assert isinstance(files, ChunkedList)
        assert list(files) == testfiles_to_match

    def test_list_files_all_query(self, client_testds: DataportalClient):

        testfiles_to_match = sorted(testfiles, key= lambda f: f['StartDate'])[1:9]
        start_date = testfiles_to_match[0]['StartDate']
        end_date = testfiles_to_match[-1]['StopDate']

        testfiles_to_match = sorted(testfiles_to_match, key= lambda f: f['MetricEntries'])

        filter_str = "%_2022%.pkl"
        filter_exclude_str = "%_2023%.pkl"
        (testfiles_match, _) = verify_files(testfiles_to_match, "MFileName", [r".*_2022.*\.pkl"])
        (_, testfiles_no_match) = verify_files([m[1] for m in testfiles_match], "MFileName", [r".*_2023.*\.pkl"])
        testfiles_to_match = [m[1] for m in testfiles_no_match]

        index = testfiles_to_match[2]['MetricEntries']

        testfiles_to_match = testfiles_to_match[:3][::-1]

        files = client_testds.listFiles(index=index, limit=2, orderBy='MetricEntries', sort='desc', startDate=start_date,
            endDate=end_date, filter=filter_str, excludeFilter=filter_exclude_str)

        assert isinstance(files, ChunkedList)
        assert list(files) == testfiles_to_match

class TestGetDataFromTestSystem:

    def test_get_data(self, client_testds: DataportalClient):
        fileid = list(client_testds.listFiles())[0]['FileID']
        df = client_testds.getData(fileid)

        entries = min(client_testds.state.file.MetricEntries, 100000)

        assert isinstance(df, pd.DataFrame)
        assert df.shape[0] == entries
        check_cache(client_testds)

class TestGetExtrafilesFromTestSystem:

    @pytest.fixture(autouse=True)
    def cleanMockDownloadDir(self):
        if Path(mockdownloaddir).is_dir():
            dsfilenames = [file['MFileName'] for file in testextrafiles]
            for filename in listdir_names(mockdownloaddir):
                if filename in dsfilenames:
                    (Path(mockdownloaddir) / filename).unlink()
        else:
            Path(mockdownloaddir).mkdir()

    def test_download_extrafile(self, client_testds: DataportalClient):
        os.chdir(mockdownloaddir)
        extrafile = next(client_testds.listFiles(extraFiles=True))
        client_testds.getExtraFiles(extrafile['FileID'])
        check_extra_files([extrafile])

    def test_download_all_extrafiles_and_test_no_input(self, client_testds: DataportalClient):
        os.chdir(mockdownloaddir)
        extrafiles = list(client_testds.listFiles(extraFiles=True))

        # Should get zero files
        client_testds.getExtraFiles()
        check_extra_files([])

        client_testds.getExtraFiles([file['FileID'] for file in extrafiles])
        check_extra_files(extrafiles)


    # test download all extrafiles

# --------------------------------------------------------------------------------------------------
# Parquet. getData decides on the file's first 4 BYTES, not on its name, so each case below differs
# only in bytes and/or name. These tests upload their OWN fixtures (the seed job runs in a later
# pipeline stage) and delete them again.
#
# Upload target: the systests_datasetname dataset ('metricdumps'). wcib's populate-deployment.sh
# creates it with user01 as OWNER, and the owner role is CASL "manage all" (deployment/wcib/
# templates/rolepermissions.yaml), which covers the 'update' (POST .../files) and 'delete'
# (DELETE .../files/<id>) checks. NOTE: 'admin' would NOT do -- it has an inverted 'delete dataset'
# rule, and DELETE of a file is checked as delete/dataset. If user01 ever loses ownership, the upload
# fails with a message saying so.
#
# CONCURRENCY: the system-tests job is a matrix (python 3.10 .. 3.13) of jobs running at the same time
# against the SAME portal and the SAME dataset. So the RUN is the unit of ownership:
#   * every file is named <prefix><upload time, base36 s><run id, 8 hex><letter>, unique per run;
#   * a run only ever deletes its own files, plus leftovers from dead runs, which are recognised by the
#     upload time in the name being older than systests_parquet_stale_age (the listing has no CreateDate);
#   * NO assertion is made about the dataset's total file count -- siblings add and remove files all the
#     time. Everything is asserted over our own FileIDs (present after upload, gone after teardown).
# --------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Permutation:
    key: str                               # test id
    letter: str                            # makes the upload name unique
    upload_ext: str                        # what the upload is named, e.g. '.parquet.zst'
    stored_suffix: str                     # what the portal's stored MFileName must END with
    make: Callable[[pd.DataFrame], bytes]  # produces the bytes
    what: str


def _parquet(compression):
    def make(df: pd.DataFrame) -> bytes:
        buf = io.BytesIO()
        df.to_parquet(buf, engine="pyarrow", compression=compression)
        return buf.getvalue()
    return make


def _zstd_wrapped_parquet(df: pd.DataFrame) -> bytes:
    # a plain parquet file compressed as a whole: outer zstd frame, 'PAR1' only appears after decompression
    return zstandard.ZstdCompressor().compress(_parquet(None)(df))


def _not_parquet(df: pd.DataFrame) -> bytes:
    return b"not a parquet file" * 20


def _csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode()


PQ_A = Permutation("a_ext_parquet_internal_zstd", "a", ".parquet", "_raw.parquet", _parquet("zstd"), "correct name, zstd is the column codec")
PQ_B = Permutation("b_ext_parquet_internal_snappy", "b", ".parquet", "_raw.parquet", _parquet("snappy"), "correct name, snappy column codec")
PQ_C = Permutation("c_ext_parquet_uncompressed", "c", ".parquet", "_raw.parquet", _parquet(None), "correct name, no compression")
# (d) and (f) are uploaded under the SAME name ('.parquet.zst') and differ only in bytes:
PQ_D = Permutation("d_parquet_zst_plain_ingester_form", "d", ".parquet.zst", "_raw.parquet.zst", _parquet("zstd"),
                   "the MQTT ingester's CURRENT, misleading name: a PLAIN PAR1 file, zstd is only the column codec (prod form)")
PQ_E = Permutation("e_zstd_parquet_spark_convention", "e", ".zstd.parquet", "_raw.zstd.parquet", _parquet("zstd"),
                   "the Spark convention wcib is moving to: plain PAR1 file with zstd column codec")
PQ_F = Permutation("f_parquet_zst_zstd_wrapped", "f", ".parquet.zst", "_raw.parquet.zst", _zstd_wrapped_parquet,
                   "the name telling the truth: parquet compressed as a whole, outer zstd frame the client must decompress")
PQ_G = Permutation("g_parquet_not_parquet", "g", ".parquet", "_raw.parquet", _not_parquet, "named parquet, but not parquet nor zstd")
CSV_H = Permutation("h_csv_control", "h", ".csv", "_raw.csv", _csv, "chunkable control for entries=")

ROUNDTRIP = [PQ_A, PQ_B, PQ_C, PQ_D, PQ_E, PQ_F]
ALL_UPLOADS = ROUNDTRIP + [PQ_G, CSV_H]


def _make_frame(rows: int) -> pd.DataFrame:
    return pd.DataFrame({
        "ts": pd.date_range("2001-01-01", periods=rows, freq="s"),
        "id": list(range(rows)),
        "value": [i * 0.5 for i in range(rows)],
        "label": [f"row-{i}" for i in range(rows)],
    })


_BASE36 = "0123456789abcdefghijklmnopqrstuvwxyz"
_STEM_RE = re.compile(re.escape(systests_parquet_prefix) + r"([0-9a-z]{7})([0-9a-f]{8})([a-z])_")


def _base36(n: int) -> str:
    digits = ""
    while n:
        n, r = divmod(n, 36)
        digits = _BASE36[r] + digits
    return digits.rjust(7, "0")


def _stale_reason(mfilename: str, now: float):
    """Why a prefixed file may be deleted by another run, or None if it must be left alone (could belong to a live sibling)."""
    m = _STEM_RE.match(mfilename)
    if m is None:
        return "name has the test prefix but no parsable upload time (predates the naming scheme)"
    age = now - int(m.group(1), 36)
    if age > systests_parquet_stale_age:
        return f"leftover of run {m.group(2)}, uploaded {age / 3600:.1f} h ago"
    return None


def _auth():
    return {"Authorization": f"Bearer {token}"}


def _listed_file_ids() -> set:
    return {f['FileID'] for f in session_get(f"v1/dataset/{testdataset['DatasetID']}/files").json()['data']}


def _upload(name: str, data: bytes, rows: int, run: str) -> int:
    start = datetime.datetime(2001, 1, 1, tzinfo=datetime.UTC)
    stop = start + datetime.timedelta(seconds=rows - 1)
    fields = {
        "start": start.strftime('%Y-%m-%dT%H:%M:%SZ'),
        "stop": stop.strftime('%Y-%m-%dT%H:%M:%SZ'),
        "count": str(rows),
        "filename": name,
        "size": str(len(data)),
        "dataflag": "raw",
        "datatype": "float",
    }
    # Content-hash idempotency: several of the files share bytes, so salt the key with run and name
    key = hashlib.sha256(run.encode() + b"\0" + name.encode() + b"\0" + data).hexdigest()
    # requests puts the form fields BEFORE the file part, the portal needs 'data' to be last
    response = s.post(
        os.path.join(os.environ["DATAPORTAL_API"], f"v1/dataset/{testdataset['DatasetID']}/files"),  # noqa: PTH118 - Path would eat the // in the URL scheme
        data=fields, files={"data": (name, data, "application/octet-stream")},
        headers={**_auth(), "Idempotency-Key": key},
    )
    if response.status_code in (401, 403):
        raise AssertionError(
            f"{systests_testuser} may not upload to {systests_datasetname} (HTTP {response.status_code}: {response.text}); "
            "the parquet system tests need user01 to be OWNER of it (see populate-deployment.sh)")
    assert response.status_code in (200, 201), f"upload of {name} failed: HTTP {response.status_code} {response.text}"
    return int(response.json()['fileId'])


def _file_ids() -> set:
    return {f['FileID'] for f in session_get(f"v1/dataset/{testdataset['DatasetID']}/files").json()['data']}


def _delete(fileid: int) -> dict:
    """Delete a file and return what the portal reported.

    The contract (wcib, see wcibdatamanager/middleware/datasetHandler.js) is:
    delete the Swift object first, then the row; a 2xx whose body says
    `objectAbsent` / `alreadyAbsent` describes what actually happened, and
    deleting an already-absent file is SUCCESS, not an error.

    This used to tolerate a 500 whose row was nevertheless gone, because the
    portal deleted the row FIRST and then removed Swift at
    "<FilePath>/<MFileName>", answering 500 when the object was not where the
    row said. wcib v1.3.2 fixed that, __api_version__ above now requires it, and
    test_delete_contract_reports_what_happened proved it live on the deployed
    release (that test went from xfail to pass the moment v1.3.2 was deployed),
    so the tolerance is gone: a 500 is a 500 again.
    """
    response = s.delete(os.path.join(os.environ["DATAPORTAL_API"], f"v1/dataset/{testdataset['DatasetID']}/files/{fileid}"), headers=_auth())  # noqa: PTH118 - Path would eat the // in the URL scheme
    response.raise_for_status()
    try:
        return response.json() or {}
    except ValueError:
        return {}


@dataclass
class UploadedFiles:
    frame: pd.DataFrame
    run: str
    ts: str          # upload time, base36 seconds
    files: dict      # Permutation.key -> the portal's file record (FileID, MFileName, ...)

    def id(self, perm: Permutation) -> int:
        return self.files[perm.key]['FileID']

    def stem(self, perm: Permutation) -> str:
        return f"{systests_parquet_prefix}{self.ts}{self.run}{perm.letter}"


@pytest.fixture(scope='class')
def uploaded():
    rows = systests_parquet_rows
    run = uuid.uuid4().hex[:8]
    frame = _make_frame(rows)

    # Leftovers of runs that died before their teardown. Only files that are provably old are touched: a
    # file with a recent embedded time may belong to a sibling matrix job that is running right now.
    now = time.time()
    for candidate in session_get(f"v1/dataset/{testdataset['DatasetID']}/files").json()['data']:
        if not candidate['MFileName'].startswith(systests_parquet_prefix):
            continue
        reason = _stale_reason(candidate['MFileName'], now)
        if reason is None:
            _logger.warning("leaving %s alone: recent, may belong to a concurrent run", candidate['MFileName'])
            continue
        _logger.warning("DELETING stale parquet systest file %s (FileID %s): %s", candidate['MFileName'], candidate['FileID'], reason)
        try:
            _delete(candidate['FileID'])
        except HTTPError as e:
            if e.response is None or e.response.status_code != 404:  # 404: another run's cleanup got there first
                raise

    result = UploadedFiles(frame=frame, run=run, ts=_base36(int(now)), files={})
    ids = []
    try:
        for perm in ALL_UPLOADS:
            fileid = _upload(f"{result.stem(perm)}{perm.upload_ext}", perm.make(frame), rows, run)
            ids.append(fileid)
            result.files[perm.key] = session_get(f"v1/dataset/{testdataset['DatasetID']}/files/{fileid}").json()
        yield result
    finally:
        failures = []
        for fileid in ids:
            try:
                _delete(fileid)
            except Exception as e:
                failures.append((fileid, e))
        assert not failures, f"could not delete uploaded files: {failures}"
        # Deliberately NOT a comparison of the dataset's total file count: the sibling matrix jobs are
        # adding and removing their own files in this dataset at the same time. Only our own ids matter.
        assert not (set(ids) & _listed_file_ids()), "files uploaded by this run are still listed after teardown"


@pytest.fixture(scope='class')
def pq_client(uploaded):
    client = DataportalClient(token, cleanStart=True).fromDataset(testdataset['DatasetName'])
    yield client
    client._stateReset()


class TestGetParquetFromTestSystem:


    def test_delete_contract_reports_what_happened(self, uploaded: UploadedFiles):
        """LIVE CHECK of the wcib removeDatafile contract, as of v1.3.2 ENFORCED.

        The contract has three parts: a delete reports WHAT it did
        (objectAbsent / alreadyAbsent), deleting an already-absent file is
        SUCCESS, and a failure leaves the row intact so a retry works.

        Until the fix was released this xfailed, naming whichever part was
        missing, so the suite stayed green against the then-deployed v1.3.1
        while still reporting the gap. wcib v1.3.2 shipped the fix, the run that
        first deployed it turned this test from xfail into pass, and
        __api_version__ now requires v1.3.2 -- so the escapes are assertions:
        a portal that regresses on any part of this FAILS here instead of
        quietly xfailing again.
        """
        frame = _make_frame(systests_parquet_rows)
        perm = PQ_A
        fileid = _upload(f"{uploaded.stem(perm)}contract{perm.upload_ext}", perm.make(frame), systests_parquet_rows, uploaded.run)

        first = _delete(fileid)   # a 500 now raises in _delete; v1.3.1 answered 500 here
        assert fileid not in _file_ids(), "the file is still listed after a successful delete"
        assert "alreadyAbsent" in first, (
            f"the portal deleted but did not report what happened (body: {first}); "
            "expected objectAbsent / alreadyAbsent from removeDatafile (wcib >= v1.3.2)"
        )
        assert first["alreadyAbsent"] is False, f"a real delete must not report alreadyAbsent: {first}"

        # Idempotent: asking again for an end state that already holds is success.
        try:
            second = _delete(fileid)
        except HTTPError as e:
            status = None if e.response is None else e.response.status_code
            raise AssertionError(
                f"a repeated DELETE must be idempotent, it returned {status} instead of 2xx with alreadyAbsent"
            ) from e
        assert second.get("alreadyAbsent") is True, f"a repeated delete must report alreadyAbsent: {second}"

    def test_uploads_are_listed_and_named_by_convention(self, uploaded: UploadedFiles):
        # The portal renames uploads to <name>_<type>_<start>_<stop>_<count>_<flag>.<ext>[.<comp>],
        # so only the stem and the suffix are ours to assert on.
        listing = session_get(f"v1/dataset/{testdataset['DatasetID']}/files").json()['data']
        listed = {f['FileID']: f for f in listing}
        # Our own ids only; the dataset's total is not ours to assert on (siblings are changing it, see above).
        assert {uploaded.id(p) for p in ALL_UPLOADS} <= set(listed)
        for perm in ALL_UPLOADS:
            record = uploaded.files[perm.key]
            assert record['FileID'] in listed, f"{perm.key} is not in the dataset listing"
            assert record['MFileName'].startswith(f"{uploaded.stem(perm)}_"), (perm.key, record['MFileName'])
            assert record['MFileName'].endswith(perm.stored_suffix), (perm.key, record['MFileName'])
            assert not record['ExtraFile']
            assert record['MetricEntries'] == systests_parquet_rows
        # (d) and (f) are the two files that share a name
        assert uploaded.files[PQ_D.key]['MFileName'].endswith('.parquet.zst')
        assert uploaded.files[PQ_F.key]['MFileName'].endswith('.parquet.zst')

    @pytest.mark.parametrize("perm", ROUNDTRIP, ids=lambda p: p.key)
    def test_parquet_roundtrip(self, pq_client: DataportalClient, uploaded: UploadedFiles, perm: Permutation):
        """(a)-(f): whatever the name and codec, getData returns the original frame.

        Compared with assert_frame_equal (values AND dtypes AND index AND column order, and a readable diff on
        failure), not a spot check: parquet is lossless for these dtypes, so any difference is a real bug,
        e.g. the wrapped (f) file being read through the wrong path.
        """
        df = pq_client.getData(uploaded.id(perm))

        assert isinstance(df, pd.DataFrame), perm.what
        pd.testing.assert_frame_equal(df, uploaded.frame)
        assert pq_client.currentLoadedFile().MFileName.endswith(perm.stored_suffix)

    def test_parquet_zst_plain_and_wrapped_share_a_name_but_not_bytes(self, pq_client: DataportalClient, uploaded: UploadedFiles):
        # (d) PLAIN parquet named .parquet.zst vs (f) zstd-WRAPPED parquet named .parquet.zst: check what the portal
        # actually stored, so a green round trip above cannot be the portal quietly altering the bytes.
        heads = {}
        for perm in (PQ_D, PQ_F):
            pq_client.getData(uploaded.id(perm))
            with (Path(pq_client.cachepath) / pq_client.state.file.MFileName).open("rb") as f:
                heads[perm.key] = f.read(4)
        assert heads[PQ_D.key] == b"PAR1"
        assert heads[PQ_F.key] == b"\x28\xb5\x2f\xfd"

    def test_not_parquet_raises(self, pq_client: DataportalClient, uploaded: UploadedFiles):
        """(g): named .parquet, but neither PAR1 nor a zstd frame -> UnsupportedExtentionError that names the first bytes."""
        with pytest.raises(UnsupportedExtentionError) as excinfo:
            pq_client.getData(uploaded.id(PQ_G))

        message = str(excinfo.value)
        assert "b'not '" in message          # the actual first four bytes
        assert "PAR1" in message             # what was expected
        assert PQ_G.stored_suffix in message  # which file

    @pytest.mark.parametrize("perm", [PQ_D, PQ_F], ids=lambda p: p.key)
    def test_compression_kwarg_is_ignored(self, pq_client: DataportalClient, uploaded: UploadedFiles, perm: Permutation, caplog):
        """compression='zstd' is what a user tries because of the '.zst' name; read_parquet has no such argument, so it is dropped (with a warning)."""
        with caplog.at_level(logging.WARNING):
            df = pq_client.getData(uploaded.id(perm), compression='zstd')

        pd.testing.assert_frame_equal(df, uploaded.frame)
        assert "ignoring compression" in caplog.text

    @pytest.mark.parametrize("perm", [PQ_D, PQ_F], ids=lambda p: p.key)
    def test_columns_passthrough(self, pq_client: DataportalClient, uploaded: UploadedFiles, perm: Permutation):
        df = pq_client.getData(uploaded.id(perm), columns=["id", "value"])

        assert list(df.columns) == ["id", "value"]
        pd.testing.assert_frame_equal(df, uploaded.frame[["id", "value"]])

    @pytest.mark.parametrize("perm", [PQ_A, PQ_F], ids=lambda p: p.key)
    def test_generator_yields_exactly_one_frame(self, pq_client: DataportalClient, uploaded: UploadedFiles, perm: Permutation):
        gen = pq_client.getData(uploaded.id(perm), generator=True)

        frames = list(gen)  # runs the generator to the end

        assert len(frames) == 1
        pd.testing.assert_frame_equal(frames[0], uploaded.frame)
        with pytest.raises(StopIteration):
            next(gen)
        # the zstd-wrapped case decompresses to a temp file next to the cache; it must be gone again
        assert [f for f in listdir_names(pq_client.cachepath) if f.endswith(".parquet.tmp")] == []

    def test_entries_does_not_truncate_parquet(self, pq_client: DataportalClient, uploaded: UploadedFiles):
        """entries= is ignored for parquet (not chunkable); the same argument does chunk a csv, which is the control."""
        parquet_df = pq_client.getData(uploaded.id(PQ_D), entries=1)
        assert parquet_df.shape[0] == systests_parquet_rows > 1

        csv_df = pq_client.getData(uploaded.id(CSV_H), entries=1)
        assert csv_df.shape[0] == 1

    def test_getdata_leaves_a_single_cached_file(self, pq_client: DataportalClient, uploaded: UploadedFiles):
        pq_client.getData(uploaded.id(PQ_F))
        check_cache(pq_client)
