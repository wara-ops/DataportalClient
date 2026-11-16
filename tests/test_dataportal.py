import base64
import gc
import gzip
import importlib.metadata
import inspect
import io
import json
import logging
import math
import os
import pickle
import re
import tarfile
import time
import urllib.parse
import weakref
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest
import requests
import zstandard
from requests.exceptions import ChunkedEncodingError, HTTPError

import dataportal
from dataportal import autherrors
from dataportal.autherrors import (
    DataportalAuthError,
    DatasetAccessDeniedError,
    TokenExpiredError,
    TokenRejectedError,
    TokenRevokedError,
    TokenScopeError,
    checkTokenExpiry,
    decodeTokenClaims,
    expiryWarning,
    expiryWarnWindow,
    explainHTTPError,
    raiseForStatus,
)
from src.dataportal.dataportalclient import ChunkedList, DataportalClient, Dataset, File, NoDatasetError, ParquetEngineMissingError, UnsupportedExtentionError, _sniffCompression
from tests.settings import (
    check_cache,
    check_extra_files,
    extrafiles,
    listdir_names,
    logfile_missingcomp,
    logfilesext,
    metricfile_missingcomp,
    metricfilesext,
    mockdatasetids,
    mockdatasetmap,
    mockdatasets,
    mockdatashapes,
    mockdownloaddir,
    mockdserr,
    mockdslogs,
    mockdsmetrics,
    mockfiles,
    mockfilesdir,
    mocktoken,
    mockurl,
    strtobool,
    testfiles,
)

os.environ["DATAPORTAL_API"] = mockurl

@pytest.fixture(scope="session", autouse=True)
def generate_testfiles():

    def readDF(filepath):
        if '.csv' in filepath:
            return pd.read_csv(filepath)
        if '.json' in filepath:
            return pd.read_json(filepath, lines=True)
        raise UnsupportedExtentionError(filepath)

    def writeDF(df, filename, extraArgs=None):
        if extraArgs is None:
            extraArgs = {}
        filepath = Path(mockfilesdir) / filename
        if '.pkl' in filename:
            df.to_pickle(filepath, **extraArgs)
        elif '.csv' in filename:
            df.to_csv(filepath, **{'index': False, **extraArgs})
        elif '.json' in filename:
            df.to_json(filepath, **{'orient': 'records', 'lines': True, **extraArgs})
        else:
            df.to_json(filepath, **{'orient': 'records', 'lines': True, **extraArgs})

    def genFilesFromDF(filepath, file_extensions, nocomp=None):
        df = readDF(filepath)
        col0 = df.columns[0]
        filename = Path(filepath).name.split('.')[0]
        for fext in file_extensions:
            newfilename = filename + fext
            newdf = df.rename(columns={col0: newfilename})
            writeDF(newdf, newfilename)

        if nocomp:
            newdf_nocomp = df.rename(columns={col0: nocomp['name']})
            writeDF(newdf_nocomp, nocomp['name'], extraArgs={'compression': nocomp['compression']})

    def createEmptyFiles(filenames: list[str]):
        for filename in filenames:
            with (Path(mockfilesdir) / filename).open('w') as fp:
                fp.write(f'This is extrafile {filename}')

    Path(mockfilesdir).mkdir(exist_ok=True)

    genFilesFromDF(testfiles['metric'], metricfilesext, metricfile_missingcomp)
    genFilesFromDF(testfiles['log'], logfilesext, logfile_missingcomp)
    createEmptyFiles([f for flist in extrafiles.values() for f in flist])

class MockedResponse:
    calls: dict[(int, int), int] = {}

    def __init__(self):
        self.status_code = 200
        self.file = None
        self.filename = ''
        self.headers = {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise HTTPError

    def json(self):
        return {}

    def iter_content(self, chunk_size=1000):
        if self.file is not None:
            while len(self.file.peek()) > 0:
                yield self.file.read(chunk_size)
        else:
            raise ValueError('No file loaded')

    def __enter__(self):
        self.file = Path(self.filename).open('rb')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()
        return

@pytest.fixture(autouse=True)
def mocked_responses(mocker):
    mockresp = MockedResponse()

    def mockfunc(url, headers=None, stream=False):
        if headers is None:
            headers = {}

        urlParts = urllib.parse.urlparse(url)
        if headers["Authorization"].split(' ')[-1] != mocktoken:
            mockresp.status_code = 400
            return mockresp

        matched = re.fullmatch('.*v1/version', urlParts.path)
        if matched:
            mockresp.content = b'v1.2.3'
            return mockresp

        matched = re.fullmatch('.*v1/validatetoken', urlParts.path)
        if matched:
           return mockresp

        matched = re.fullmatch('.*v1/dataset', urlParts.path)
        if matched:
           mockresp.json = lambda: deepcopy(mockdatasets)
           return mockresp

        # list file
        matched = re.fullmatch('.*dataset/(?P<datasetid>[0-9]+)/files', urlParts.path)
        if matched:
            items = mockfiles[int(matched.groupdict()['datasetid'])]
            response = {
                'data': items,
                'pagination': {
                    'total_records': len(items),
                    'count': len(items),
                    'limit': 500,
                    'prev_page': {},
                    'next_page': {},
                    'current_page': {
                        'index': items[0]['FileID'],
                        'direction': 'next'
                    }
                }
            }
            mockresp.json = lambda: response
            return mockresp

        # get file info
        matched = re.fullmatch('.*dataset/(?P<datasetid>[0-9]+)/files/(?P<fileid>[0-9]+)', url)
        if matched:
            gd = matched.groupdict()
            datasetid = int(gd['datasetid'])
            fileid = int(gd['fileid'])

            files = list(filter(lambda f: f['FileID'] == fileid, mockfiles[datasetid]))

            if len(files) == 0:
                mockresp.status_code = 404
                raise HTTPError(response=mockresp)

            mockresp.json = lambda: deepcopy(files[0])

            return mockresp

        # get file content
        matched = re.fullmatch('.*dataset/(?P<datasetid>[0-9]+)/files/(?P<fileid>[0-9]+)/content', urlParts.path)
        if matched:
            gd = matched.groupdict()
            datasetid = int(gd['datasetid'])
            fileid = int(gd['fileid'])

            query = urllib.parse.parse_qs(urlParts.query)
            query = {k: v[0] for k,v in query.items()}

            if (datasetid, fileid) not in mockresp.calls:
                mockresp.calls[(datasetid, fileid)] = 1
            else:
                mockresp.calls[(datasetid, fileid)] += 1

            try:
                if 'extrafile' in query and strtobool(query['extrafile']):
                    file = next(filter(lambda fo: fo['FileID'] == fileid and fo['ExtraFile'], mockfiles[datasetid]))
                else:
                    file = next(filter(lambda fo: fo['FileID'] == fileid and not fo['ExtraFile'], mockfiles[datasetid]))
                filename = file['MFileName']
            except StopIteration:
                mockresp.status_code = 404
                raise HTTPError(response=mockresp) from None
            except Exception as e:
                print("error: get content, ", e)
                mockresp.status_code = 500
                raise HTTPError(response=mockresp) from e

            if (datasetid == mockdatasetids[mockdserr]):
                if file['OriginName'] == 'HTTPError':
                    raise HTTPError
                if file['OriginName'] == 'ChunkedEncodingError1' and mockresp.calls[(datasetid, fileid)] == 1:
                    raise ChunkedEncodingError
                if file['OriginName'] == 'ChunkedEncodingErrorX':
                    raise ChunkedEncodingError
                if file['OriginName'] == 'Exception':
                    raise Exception('mocked exception')

            mockresp.headers = {"Content-disposition": "filename=" + filename}
            mockresp.filename = str(Path(mockfilesdir) / filename)

            print(mockresp.filename)
            print(mockresp.calls)

            return mockresp

        raise Exception(f'No url matches on {url}')

    mocker.patch("requests.Session.get", side_effect=mockfunc)
    return

@pytest.fixture
def client_clean():
    return DataportalClient(mocktoken, cleanStart=True)



class TestPackageVersion:
    def test_version_is_the_installed_distribution_version(self):
        # __version__ must come from the installed metadata (which pdm-backend derived
        # from the git tag), not from a file that can drift from the tag. It is the
        # PEP 440 form, so '1.2.3' for the tag v1.2.3: no leading 'v'.
        assert dataportal.__version__ == importlib.metadata.version("dataportalclient")

    def test_api_version_is_not_part_of_the_package_surface(self):
        assert not hasattr(dataportal, "__api_version__")


class TestCreateClient:
    def test_create_client(self, caplog):
        with caplog.at_level(logging.INFO):
            DataportalClient(mocktoken)
            assert "Connection OK" in caplog.text

    def test_create_clean_client(self, caplog):
        with caplog.at_level(logging.INFO):
            DataportalClient(mocktoken, cleanStart=True)
            assert "Connection OK" in caplog.text

    def test_wrong_API_version_major(self, caplog, mocker):
        mocker.patch('src.dataportal.DataportalClient._getAPIReqVersion', side_effect=lambda: 'v0.0.0')
        with caplog.at_level(logging.INFO):
            DataportalClient(mocktoken)
            assert "Major version missmatch" in caplog.text

    def test_wrong_API_version_minor(self, caplog, mocker):
        mocker.patch('src.dataportal.DataportalClient._getAPIReqVersion', side_effect=lambda: 'v1.3.0')
        with caplog.at_level(logging.INFO):
            DataportalClient(mocktoken)
            assert "Assumed minor version larger" in caplog.text

    def test_wrong_token(self):
        with pytest.raises(HTTPError):
            DataportalClient('badtoken')

class TestLoadDataset:
    def test_load_dataset_with_name(self, client_clean: DataportalClient):
        client_loaded = client_clean.fromDataset(mockdsmetrics)
        assert isinstance(client_loaded, DataportalClient)
        assert client_loaded == client_clean
        assert client_loaded.state.dataset.DatasetName == mockdsmetrics
        assert isinstance(client_loaded.state.dataset.DatasetID, int)

    def test_load_dataset_with_ID(self, client_clean: DataportalClient):
        client_loaded = client_clean.fromDataset(0)
        assert isinstance(client_loaded, DataportalClient)
        assert client_loaded == client_clean
        assert client_loaded.state.dataset.DatasetName == mockdsmetrics

    def test_get_current_dataset(self, client_clean: DataportalClient):
        client_loaded =  client_clean.fromDataset(mockdsmetrics)
        ds = client_loaded.currentDataset()
        to_match =  [ds for ds in mockdatasets['Datasets'] if ds['DatasetName'] == mockdsmetrics].pop()
        assert json.loads(ds.model_dump_json()) == to_match

    def test_nonexistent_dataset(self, client_clean: DataportalClient):
        with pytest.raises(ValueError):
            client_clean.fromDataset('doesnotexist')

# listFiles() is more extensively tested in the systemtests due to it being a iterator
# wrapping mostly passthroughs to the API, and thus cumbersome to mock properly.
class TestListFiles:

    def test_no_loaded_dataset(self, client_clean: DataportalClient):
        with pytest.raises(NoDatasetError):
            client_clean.listFiles()

    def test_list_files(self, client_clean: DataportalClient, capsys):
        files = client_clean.fromDataset(mockdsmetrics).listFiles()
        captured = capsys.readouterr()
        assert isinstance(files, ChunkedList)
        assert len(files) == len(mockfiles[mockdatasetids[mockdsmetrics]])
        assert captured.out == ''

    def test_list_files_iterator_over_local_data(self, client_clean: DataportalClient):
        files = client_clean.fromDataset(mockdsmetrics).listFiles()
        testfiles = mockfiles[mockdatasetids[mockdsmetrics]]

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

class TestDownloadFile:

    @pytest.fixture
    def client(self, client_clean: DataportalClient):
        client = client_clean.fromDataset(mockdserr)
        client.reconnectWaitTime = 0.1
        return client

    @pytest.fixture(scope="class", autouse=True)
    def cleanMockDownloadDir(self):
        if Path(mockdownloaddir).is_dir():
            dsfilenames = [file['MFileName'] for file in mockfiles[mockdatasetids[mockdserr]]]
            for filename in listdir_names(mockdownloaddir):
                if filename in dsfilenames:
                    (Path(mockdownloaddir) / filename).unlink()
        else:
            Path(mockdownloaddir).mkdir()

    def test_download_file(self, client: DataportalClient):
        file = next(filter(lambda o: o['OriginName'] == 'thisworks', client.listFiles()))

        client._downloadFile(file['FileID'], mockdownloaddir)

        assert len(listdir_names(mockdownloaddir)) > 0
        assert file['MFileName'] in listdir_names(mockdownloaddir)
        assert (Path(mockdownloaddir) / file['MFileName']).stat().st_size > 0


    def test_download_file_retries(self, client: DataportalClient, caplog):

        with caplog.at_level(logging.INFO):
            file = next(filter(lambda o: o['OriginName'] == 'ChunkedEncodingError1', client.listFiles()))

            client._downloadFile(file['FileID'], mockdownloaddir)

            # assert that 1 error has been thrown
            strmatch = f'Connection failed (1/{client.reconnectAttemps}), retrying...'
            assert strmatch in caplog.text

            assert len(listdir_names(mockdownloaddir)) > 0
            assert file['MFileName'] in listdir_names(mockdownloaddir)
            assert (Path(mockdownloaddir) / file['MFileName']).stat().st_size > 0

    def test_download_file_too_many_retries(self, client: DataportalClient, caplog):
        with caplog.at_level(logging.INFO):
            file = next(filter(lambda o: o['OriginName'] == 'ChunkedEncodingErrorX', client.listFiles()))

            with pytest.raises(Exception, match="ChunkedEncodingError"):
                client._downloadFile(file['FileID'], mockdownloaddir)

            # assert that all recconection attempts have been made
            strmatch = f"Connection failed ({client.reconnectAttemps}/{client.reconnectAttemps}), throwing error"
            assert strmatch in caplog.text

            # file has not been downloaded
            assert file['MFileName'] not in listdir_names(mockdownloaddir)

    def test_download_file_general_exception_handling(self, client: DataportalClient):
        file = next(filter(lambda o: o['OriginName'] == 'Exception', client.listFiles()))

        with pytest.raises(Exception) as e:
            client._downloadFile(file['FileID'], mockdownloaddir)

        # the correct error message has been returned in exception
        assert str(e.value) == 'mocked exception'

        # file has not been downloaded
        assert file['MFileName'] not in listdir_names(mockdownloaddir)


class TestCacheDataFile:

    @pytest.fixture
    def client(self, client_clean: DataportalClient):
        return client_clean.fromDataset(mockdsmetrics)

    @pytest.fixture
    def client_preloaded(self):
        return DataportalClient(mocktoken).fromDataset(mockdsmetrics)

    def test_cache_new_file(self, client: DataportalClient):
        datafiles = filter(lambda o: not o['ExtraFile'], client.listFiles())
        file = next(datafiles)
        client._cacheDataFile(file['FileID'])

        assert File(**file) == client.state.file
        check_cache(client)

    def test_new_client_loads_cached_file(self, client_preloaded: DataportalClient):
        check_cache(client_preloaded)

    def test_cache_extrafile_throws_error_keeps_state(self, client_preloaded: DataportalClient):
        extrafile = next(filter(lambda o: o['ExtraFile'], client_preloaded.listFiles(extraFiles=True)))
        prevState = deepcopy(client_preloaded.state)
        with pytest.raises(ValueError):
            client_preloaded._cacheDataFile(extrafile['FileID'])

        assert prevState == client_preloaded.state
        check_cache(client_preloaded)

    def test_cache_another_file_overwrites_old(self, client_preloaded: DataportalClient):
        oldFileState = client_preloaded.state.file

        datafiles = filter(lambda o: not o['ExtraFile'], client_preloaded.listFiles())
        next(datafiles)
        file = next(datafiles)
        client_preloaded._cacheDataFile(file['FileID'])

        assert File(**file) == client_preloaded.state.file
        assert oldFileState != client_preloaded.state.file

        check_cache(client_preloaded)

    def test_current_file(self, client_preloaded: DataportalClient):
        df = client_preloaded.currentLoadedFile()
        assert File(**json.loads(df.model_dump_json())) == client_preloaded.state.file

    def test_client_clean_state(self, client_preloaded: DataportalClient):
        assert len(listdir_names(client_preloaded.cachepath)) == 2

        client_clean = DataportalClient(mocktoken, cleanStart=True)
        assert client_clean.state.dataset is None
        assert client_clean.state.file is None
        assert len(listdir_names(client_preloaded.cachepath)) == 0

    def test_error_during_download_resets_file(self, client_clean: DataportalClient):
        # Load initial state
        client_clean.fromDataset(mockdserr)
        file = next(filter(lambda o: o['OriginName'] == 'thisworks', client_clean.listFiles()))
        client_clean.getData(file['FileID'])
        check_cache(client_clean)

        # Try to load file that throws error while downloading
        prevstate = deepcopy(client_clean.state)
        fileErr = next(filter(lambda o: o['OriginName'] == 'HTTPError', client_clean.listFiles()))
        with pytest.raises(HTTPError):
            client_clean.getData(fileErr['FileID'])

        check_cache(client_clean, dataFileCached=False)

        # Check that the file has been reset
        assert prevstate.dataset == client_clean.state.dataset
        assert prevstate.file != client_clean.state.file
        assert client_clean.state.file is None

class TestGetData:

    def test_no_loaded_dataset(self, client_clean: DataportalClient):
        with pytest.raises(NoDatasetError):
            client_clean.getData('1')

    def test_get_nonexistent_fileid_error_throw(self, client_clean: DataportalClient):
        client_clean.fromDataset(mockdsmetrics)
        fileiderr = mockfiles[mockdatasetids[mockdsmetrics]][-1]['FileID'] + 1
        with pytest.raises(ValueError) as e:
            client_clean.getData(fileiderr)

        assert str(e.value) == client_clean._errTextNoFile(Dataset(**mockdatasetmap[mockdsmetrics]), fileiderr)

    def test_get_extrafile_error_throw(self, client_clean: DataportalClient):
        client_clean.fromDataset(mockdsmetrics)
        extrafile = next(filter(lambda o: o['ExtraFile'], client_clean.listFiles(extraFiles=True)))
        with pytest.raises(ValueError):
            client_clean.getData(extrafile['FileID'])
        assert len(listdir_names(client_clean.cachepath)) == 0
        assert client_clean.state.file is None


    def test_get_data(self, client_clean: DataportalClient):
        client_clean.fromDataset(mockdsmetrics)
        file = next(filter(
            lambda o: '.csv' in o['MFileName'],
            client_clean.listFiles())
        )
        df = client_clean.getData(file['FileID'])

        assert isinstance(df, pd.DataFrame)
        assert df.shape == mockdatashapes['metric']
        check_cache(client_clean)

    def test_get_with_extraargs(self, client_clean: DataportalClient):
        client_clean.fromDataset(mockdsmetrics)
        file = next(filter(
            lambda o: '.csv' in o['MFileName'],
            client_clean.listFiles())
        )

        nrows = 2
        dtype = 'string'
        df = client_clean.getData(file['FileID'], nrows=nrows, dtype=dtype)

        assert isinstance(df, pd.DataFrame)
        assert df.dtypes.values[0] == dtype
        assert df.columns[0] == file['MFileName']
        assert df.shape[0] == nrows
        assert df.shape[1] == mockdatashapes['metric'][1]
        check_cache(client_clean)

    @pytest.mark.parametrize("fext", metricfilesext)
    def test_load_metric_file(self, client_clean: DataportalClient, fext):
        client_clean.fromDataset(mockdsmetrics)
        file = next(filter(
            lambda o: fext in o['MFileName'],
            client_clean.listFiles())
        )
        df = client_clean.getData(file['FileID'])
        assert isinstance(df, pd.DataFrame)
        assert df.columns[0] == file['MFileName']
        assert df.shape == mockdatashapes['metric']
        check_cache(client_clean)

    @pytest.mark.parametrize("fext", list(filter(lambda e: 'pkl' not in e, metricfilesext)))
    def test_load_metric_file_as_generator(self, client_clean: DataportalClient, fext):
        entriesToRead = 3

        client_clean.fromDataset(mockdsmetrics)
        file = next(filter(
            lambda o: fext in o['MFileName'],
            client_clean.listFiles())
        )
        dfgen = client_clean.getData(file['FileID'], entries=entriesToRead, generator=True)
        assert isinstance(dfgen, pd.io.parsers.readers.TextFileReader)

        c = 0
        for df in dfgen:
            c += 1
            assert isinstance(df, pd.DataFrame)
            assert df.columns[0] == file['MFileName']
            assert df.shape[0] == entriesToRead or mockdatashapes['metric'][0] % entriesToRead
            assert df.shape[1] == mockdatashapes['metric'][1]

        assert c == math.ceil(mockdatashapes['metric'][0] / entriesToRead)
        check_cache(client_clean)

    @pytest.mark.parametrize("fext", ['', '.json'] ) #)logfilesext)
    def test_load_log_file(self, client_clean: DataportalClient, fext):
        client_clean.fromDataset(mockdslogs)
        file = next(filter(
            lambda o: fext in o['MFileName'],
            client_clean.listFiles())
        )
        df = client_clean.getData(file['FileID'])
        assert isinstance(df, pd.DataFrame)
        assert df.columns[0] == file['MFileName']
        assert df.shape == mockdatashapes['log']
        check_cache(client_clean)

    @pytest.mark.parametrize("fext", logfilesext)
    def test_load_log_file_as_generator(self, client_clean: DataportalClient, fext):
        entriesToRead = 3

        client_clean.fromDataset(mockdslogs)
        file = next(filter(
            lambda o: fext in o['MFileName'],
            client_clean.listFiles())
        )
        dfgen = client_clean.getData(file['FileID'], entries=entriesToRead, generator=True)
        assert isinstance(dfgen, pd.io.json._json.JsonReader)

        c = 0
        for df in dfgen:
            c += 1
            assert isinstance(df, pd.DataFrame)
            assert df.columns[0] == file['MFileName']
            assert df.shape[0] == entriesToRead or mockdatashapes['log'][0] % entriesToRead
            assert df.shape[1] == mockdatashapes['log'][1]

        assert c == math.ceil(mockdatashapes['log'][0] / entriesToRead)
        check_cache(client_clean)

class TestGetExtrafiles:

    @pytest.fixture
    def client(self, client_clean: DataportalClient):
        return client_clean.fromDataset(mockdsmetrics)

    @pytest.fixture(autouse=True)
    def cleanMockDownloadDir(self):
        if Path(mockdownloaddir).is_dir():
            dsfilenames = [file['MFileName'] for file in mockfiles[mockdatasetids[mockdsmetrics]]]
            for filename in listdir_names(mockdownloaddir):
                if filename in dsfilenames:
                    (Path(mockdownloaddir) / filename).unlink()
        else:
            Path(mockdownloaddir).mkdir()

    @pytest.fixture(autouse=True)
    def returnToTestDir(self):
        testdir = Path.cwd()
        yield
        os.chdir(testdir)

    def test_no_loaded_dataset(self, client_clean: DataportalClient):
        with pytest.raises(NoDatasetError):
            client_clean.getExtraFiles('1')

    def test_download_extrafile(self, client: DataportalClient):
        os.chdir(mockdownloaddir)
        extrafile = next(filter(lambda o: o['ExtraFile'], client.listFiles(extraFiles=True)))
        client.getExtraFiles(extrafile['FileID'])
        check_extra_files([extrafile])

    def test_download_extrafile_with_mixed_list(self, client: DataportalClient):
        os.chdir(mockdownloaddir)
        extrafiles = filter(lambda o: o['ExtraFile'], client.listFiles(extraFiles=True))
        try:
            ef1 = next(extrafiles)
            ef2 = next(extrafiles)
        except StopIteration as e:
            raise ValueError('need atleast 2 extrafiles in selected dataset for this test') from e

        client.getExtraFiles([ef1['FileID'], f"{ef2['FileID']}"])
        check_extra_files([ef1, ef2])


    def test_download_all_extrafiles_and_test_no_input(self, client: DataportalClient):
        os.chdir(mockdownloaddir)
        extrafiles = list(filter(lambda o: o['ExtraFile'], client.listFiles(extraFiles=True)))

        # Should get zero files
        client.getExtraFiles()
        check_extra_files([])

        client.getExtraFiles([file['FileID'] for file in extrafiles])
        check_extra_files(extrafiles)


    def test_download_extrafile_missing_fileid(self, client: DataportalClient):
        os.chdir(mockdownloaddir)
        files = client.listFiles()
        extrafile = next(filter(lambda o: o['ExtraFile'], client.listFiles(extraFiles=True)))

        nonexistent_id = max([f['FileID'] for f in files])+1

        with pytest.raises(ValueError) as e:
            ids, failures = client.getExtraFiles([extrafile['FileID'], nonexistent_id])
            raise failures[nonexistent_id]

        assert str(e.value) == client._errTextNoFile(Dataset(**mockdatasetmap[mockdsmetrics]), nonexistent_id)

        # No file downloaded
        assert len(listdir_names(mockdownloaddir)) == 1

    def test_download_extrafile_raise_error_on_datafile(self, client: DataportalClient):
        os.chdir(mockdownloaddir)
        datafile = next(filter(lambda file: not file['ExtraFile'], client.listFiles(extraFiles=True)))

        with pytest.raises(ValueError):
            ids, failures = client.getExtraFiles(datafile['FileID'])
            raise failures[datafile['FileID']]

        # No file downloaded
        assert len(listdir_names(mockdownloaddir)) == 0

    def test_download_extrafile_bad_string(self, client: DataportalClient):
        os.chdir(mockdownloaddir)

        with pytest.raises(ValueError):
            ids, failures = client.getExtraFiles("hej")

        # No file downloaded
        assert len(listdir_names(mockdownloaddir)) == 0

        with pytest.raises(ValueError):
            ids, failures = client.getExtraFiles(["hej"])

        # No file downloaded
        assert len(listdir_names(mockdownloaddir)) == 0


class TestGetDataParquet:
    """
    Parquet files are not part of the generated metric/log file matrix (that would multiply every
    file-listing expectation), so each test writes its own file into the mock file directory and
    temporarily registers it in the mock dataset listing.
    """

    df = pd.DataFrame({
        'ts': pd.date_range('2026-10-06T09:10:32', periods=7, freq='s'),
        'value': [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5],
        'topic': ['a', 'b', 'c', 'a', 'b', 'c', 'a'],
    })

    @pytest.fixture
    def addparquet(self):
        mockds = mockfiles[mockdatasetids[mockdsmetrics]]
        added = []

        def add(filename: str, content: bytes):
            with (Path(mockfilesdir) / filename).open('wb') as f:
                f.write(content)
            fileid = max(fo['FileID'] for fl in mockfiles.values() for fo in fl) + 1
            fileentry = {
                "FileID": fileid,
                "MFileName": filename,
                "OriginName": 'somedatasource',
                "StartDate": '2026-10-06T00:00:00Z',
                "StopDate": '2026-10-07T00:00:00Z',
                "FileSize": len(content),
                "MetricEntries": len(self.df),
                "MetricType": 'float',
                "ExtraFile": False
            }
            mockds.append(fileentry)
            added.append(fileentry)
            return fileid

        yield add

        for fileentry in added:
            mockds.remove(fileentry)
            (Path(mockfilesdir) / fileentry['MFileName']).unlink()

    @staticmethod
    def parquetbytes(df, tmp_path, **kwargs):
        path = tmp_path / 'written.parquet'
        df.to_parquet(path, **kwargs)
        return path.read_bytes()

    def test_ingester_style_parquet_zst(self, client_clean: DataportalClient, addparquet, tmp_path):
        # exactly how the portal's MQTT ingester writes: to_parquet(compression='zstd'), named .parquet.zst
        content = self.parquetbytes(self.df, tmp_path, compression='zstd')
        assert content[:4] == b'PAR1'  # plain parquet, zstd is only the column codec
        fileid = addparquet('mqtt_mixed_2026-10-06T09:10:32.508_raw.parquet.zst', content)

        client_clean.fromDataset(mockdsmetrics)
        df = client_clean.getData(fileid)
        pd.testing.assert_frame_equal(df, self.df)
        check_cache(client_clean)

    def test_plain_parquet(self, client_clean: DataportalClient, addparquet, tmp_path):
        fileid = addparquet('plainfile.parquet', self.parquetbytes(self.df, tmp_path))

        client_clean.fromDataset(mockdsmetrics)
        df = client_clean.getData(fileid)
        pd.testing.assert_frame_equal(df, self.df)
        check_cache(client_clean)

    def test_zstd_wrapped_parquet(self, client_clean: DataportalClient, addparquet, tmp_path):
        content = zstandard.ZstdCompressor().compress(self.parquetbytes(self.df, tmp_path))
        assert content[:4] == b'\x28\xb5\x2f\xfd'
        fileid = addparquet('wrapped.parquet.zst', content)

        client_clean.fromDataset(mockdsmetrics)
        df = client_clean.getData(fileid)
        pd.testing.assert_frame_equal(df, self.df)
        # the temporary decompressed copy must be gone, only the cached file and statefile remain
        check_cache(client_clean)

    def test_unrecognised_bytes_error(self, client_clean: DataportalClient, addparquet):
        fileid = addparquet('notparquet.parquet', b'hello, this is not parquet')

        client_clean.fromDataset(mockdsmetrics)
        with pytest.raises(UnsupportedExtentionError) as e:
            client_clean.getData(fileid)
        assert "b'hell'" in str(e.value)
        assert 'notparquet.parquet' in str(e.value)

    def test_generator_yields_single_frame(self, client_clean: DataportalClient, addparquet, tmp_path):
        fileid = addparquet('gen.parquet.zst', self.parquetbytes(self.df, tmp_path, compression='zstd'))

        client_clean.fromDataset(mockdsmetrics)
        # 'entries' does not apply to parquet, so the whole file comes back as one frame
        dfgen = client_clean.getData(fileid, entries=2, generator=True)
        assert inspect.isgenerator(dfgen)

        dfs = list(dfgen)
        assert len(dfs) == 1
        pd.testing.assert_frame_equal(dfs[0], self.df)
        check_cache(client_clean)

    def test_entries_ignored(self, client_clean: DataportalClient, addparquet, tmp_path):
        fileid = addparquet('entries.parquet', self.parquetbytes(self.df, tmp_path))

        client_clean.fromDataset(mockdsmetrics)
        assert client_clean.getData(fileid, entries=2).shape == self.df.shape

    @pytest.mark.parametrize("wrapped", [False, True])
    def test_extraargs_passthrough(self, client_clean: DataportalClient, addparquet, tmp_path, wrapped):
        content = self.parquetbytes(self.df, tmp_path, compression='zstd')
        if wrapped:
            content = zstandard.ZstdCompressor().compress(content)
        fileid = addparquet('columns.parquet.zst', content)

        client_clean.fromDataset(mockdsmetrics)
        df = client_clean.getData(fileid, columns=['topic', 'value'])
        assert list(df.columns) == ['topic', 'value']
        pd.testing.assert_frame_equal(df, self.df[['topic', 'value']])

    def test_missing_engine_error(self, client_clean: DataportalClient, addparquet, tmp_path, mocker):
        fileid = addparquet('noengine.parquet.zst', self.parquetbytes(self.df, tmp_path, compression='zstd'))
        mocker.patch('pandas.read_parquet', side_effect=ImportError('Unable to find a usable engine'))

        client_clean.fromDataset(mockdsmetrics)
        with pytest.raises(ParquetEngineMissingError) as e:
            client_clean.getData(fileid)
        assert 'pip install pyarrow' in str(e.value)
        assert 'noengine.parquet.zst' in str(e.value)


@pytest.fixture
def addmockfile():
    """Writes a file into the mock file directory and temporarily registers it in the metrics dataset listing."""
    mockds = mockfiles[mockdatasetids[mockdsmetrics]]
    added = []

    def add(filename: str, content: bytes):
        with (Path(mockfilesdir) / filename).open('wb') as f:
            f.write(content)
        fileid = max(fo['FileID'] for fl in mockfiles.values() for fo in fl) + 1
        fileentry = {
            "FileID": fileid,
            "MFileName": filename,
            "OriginName": 'somedatasource',
            "StartDate": '2026-10-06T00:00:00Z',
            "StopDate": '2026-10-07T00:00:00Z',
            "FileSize": len(content),
            "MetricEntries": 7,
            "MetricType": 'float',
            "ExtraFile": False
        }
        mockds.append(fileentry)
        added.append(fileentry)
        return fileid

    yield add

    for fileentry in added:
        mockds.remove(fileentry)
        (Path(mockfilesdir) / fileentry['MFileName']).unlink()


_COMPRESSION_DF = pd.DataFrame({
    'ts': pd.date_range('2026-10-06T09:10:32', periods=7, freq='s'),
    'value': [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5],
    'topic': ['a', 'b', 'c', 'a', 'b', 'c', 'a'],
})


class TestGetDataEntries:
    """entries=-1 reads the whole file, other values below 1 are rejected, positive values chunk."""

    df = _COMPRESSION_DF

    @pytest.fixture(params=['csv', 'json'])
    def fileid(self, request, client_clean: DataportalClient, addmockfile):
        if request.param == 'csv':
            content = self.df.to_csv(index=False).encode()
            fileid = addmockfile('entries_all.csv', content)
        else:
            content = self.df.to_json(orient='records', lines=True, date_format='iso').encode()
            fileid = addmockfile('entries_all.json', content)
        client_clean.fromDataset(mockdsmetrics)
        return fileid

    def test_all_entries_dataframe(self, client_clean: DataportalClient, fileid):
        df = client_clean.getData(fileid, entries=-1)
        assert isinstance(df, pd.DataFrame)
        assert df.shape == self.df.shape
        assert list(df['value']) == list(self.df['value'])
        check_cache(client_clean)

    def test_all_entries_generator(self, client_clean: DataportalClient, fileid):
        dfgen = client_clean.getData(fileid, entries=-1, generator=True)
        assert inspect.isgenerator(dfgen)
        dfs = list(dfgen)
        assert len(dfs) == 1
        assert dfs[0].shape == self.df.shape
        assert list(dfs[0]['topic']) == list(self.df['topic'])

    def test_all_entries_with_extraargs(self, client_clean: DataportalClient, fileid):
        assert len(client_clean.getData(fileid, entries=-1, nrows=2)) == 2

    @pytest.mark.parametrize("entries", [0, -2, -100, 1.5, 3.0, '3', None, True])
    def test_invalid_entries_rejected(self, client_clean: DataportalClient, fileid, entries):
        with pytest.raises(ValueError) as e:
            client_clean.getData(fileid, entries=entries)
        assert '>= 1' in str(e.value)
        assert '-1' in str(e.value)

    @pytest.mark.parametrize("entries", [0, -2])
    def test_invalid_entries_rejected_before_download(self, client_clean: DataportalClient, addmockfile, entries):
        fileid = addmockfile('entries_nodl.csv', self.df.to_csv(index=False).encode())
        client_clean.fromDataset(mockdsmetrics)
        with pytest.raises(ValueError):
            client_clean.getData(fileid, entries=entries)
        assert len(listdir_names(client_clean.cachepath)) == 0
        assert client_clean.state.file is None

    def test_positive_entries_chunk(self, client_clean: DataportalClient, fileid):
        df = client_clean.getData(fileid, entries=3)
        assert df.shape[0] == 3

        sizes = [len(d) for d in client_clean.getData(fileid, entries=3, generator=True)]
        assert sizes == [3, 3, 1]

    def test_positive_entries_larger_than_file(self, client_clean: DataportalClient, fileid):
        assert len(client_clean.getData(fileid, entries=1000)) == len(self.df)

    def test_all_entries_unknown_extension_json_fallback(self, client_clean: DataportalClient, addmockfile):
        fileid = addmockfile('entries_noext', self.df.to_json(orient='records', lines=True, date_format='iso').encode())
        client_clean.fromDataset(mockdsmetrics)
        dfs = list(client_clean.getData(fileid, entries=-1, generator=True))
        assert len(dfs) == 1
        assert len(dfs[0]) == len(self.df)

    def test_all_entries_unknown_extension_unparsable(self, client_clean: DataportalClient, addmockfile):
        fileid = addmockfile('entries_garbage', b'this is not json\nat all')
        client_clean.fromDataset(mockdsmetrics)
        with pytest.raises(UnsupportedExtentionError):
            client_clean.getData(fileid, entries=-1)

    def test_pickle_ignores_entries(self, client_clean: DataportalClient, addmockfile, tmp_path):
        path = tmp_path / 'x.pkl'
        self.df.to_pickle(path)
        fileid = addmockfile('entries_pickle.pkl', path.read_bytes())
        client_clean.fromDataset(mockdsmetrics)
        pd.testing.assert_frame_equal(client_clean.getData(fileid, entries=-1), self.df)


class TestGetDataCompressionDetection:
    """Compression is detected from the first bytes, whatever the file is named."""

    df = _COMPRESSION_DF

    @staticmethod
    def pickled(df, tmp_path, compression):
        path = tmp_path / 'written.pkl'
        df.to_pickle(path, compression=compression)
        return path.read_bytes()

    @pytest.mark.parametrize("compression, magic", [
        ('bz2', b'BZh'),
        ('gzip', b'\x1f\x8b'),
        ('xz', b'\xfd7zXZ\x00'),
        ('zstd', b'\x28\xb5\x2f\xfd'),
        ('zip', b'PK\x03\x04'),
        (None, b'\x80'),
    ])
    def test_pickle_named_pkl(self, client_clean: DataportalClient, addmockfile, tmp_path, compression, magic):
        content = self.pickled(self.df, tmp_path, compression)
        assert content.startswith(magic)
        fileid = addmockfile(f'history_{compression}.pkl', content)

        client_clean.fromDataset(mockdsmetrics)
        pd.testing.assert_frame_equal(client_clean.getData(fileid), self.df)
        check_cache(client_clean)

    def test_pickle_zstd_named_pkl_zstd(self, client_clean: DataportalClient, addmockfile, tmp_path):
        # pandas only maps '.zst', so this name used to give 'pickle data was truncated'
        fileid = addmockfile('history_zstdname.pkl.zstd', self.pickled(self.df, tmp_path, 'zstd'))

        client_clean.fromDataset(mockdsmetrics)
        pd.testing.assert_frame_equal(client_clean.getData(fileid), self.df)

    def test_pickle_generator(self, client_clean: DataportalClient, addmockfile, tmp_path):
        fileid = addmockfile('history_gen.pkl', self.pickled(self.df, tmp_path, 'bz2'))

        client_clean.fromDataset(mockdsmetrics)
        dfs = list(client_clean.getData(fileid, generator=True))
        assert len(dfs) == 1
        pd.testing.assert_frame_equal(dfs[0], self.df)

    def test_explicit_compression_is_honoured(self, client_clean: DataportalClient, addmockfile, tmp_path):
        fileid = addmockfile('explicit.pkl', self.pickled(self.df, tmp_path, 'bz2'))

        client_clean.fromDataset(mockdsmetrics)
        # the matching explicit value works (the tutorials do this)
        pd.testing.assert_frame_equal(client_clean.getData(fileid, compression='bz2'), self.df)
        # a wrong explicit value is NOT overridden by the detection: bz2 data read as gzip fails
        with pytest.raises(OSError):
            client_clean.getData(fileid, compression='gzip')
        # nor is an explicit None: the compressed bytes are handed to pickle as they are
        with pytest.raises(pickle.UnpicklingError):
            client_clean.getData(fileid, compression=None)

    def test_explicit_compression_passed_to_pandas_unchanged(self, client_clean: DataportalClient, addmockfile, tmp_path, mocker):
        fileid = addmockfile('passthrough.pkl', self.pickled(self.df, tmp_path, 'bz2'))
        spy = mocker.patch('pandas.read_pickle', return_value=self.df)

        client_clean.fromDataset(mockdsmetrics)
        client_clean.getData(fileid, compression=None)
        assert spy.call_args.kwargs == {'compression': None}
        client_clean.getData(fileid)
        assert spy.call_args.kwargs == {'compression': 'bz2'}

    def test_plain_file_gets_no_compression_argument(self, client_clean: DataportalClient, addmockfile, tmp_path, mocker):
        fileid = addmockfile('plain.pkl', self.pickled(self.df, tmp_path, None))
        spy = mocker.patch('pandas.read_pickle', return_value=self.df)

        client_clean.fromDataset(mockdsmetrics)
        client_clean.getData(fileid)
        assert spy.call_args.kwargs == {}

    def test_logs_detected_compression(self, client_clean: DataportalClient, addmockfile, tmp_path, caplog):
        fileid = addmockfile('logged.pkl', self.pickled(self.df, tmp_path, 'bz2'))

        client_clean.fromDataset(mockdsmetrics)
        with caplog.at_level(logging.DEBUG, logger='dataportal.dataportalclient'):
            client_clean.getData(fileid)
        assert any('detected compression bz2' in r.getMessage() and r.levelno == logging.DEBUG for r in caplog.records)

    @pytest.mark.parametrize("mode", ['w', 'w:gz', 'w:bz2', 'w:xz'])
    def test_tar_archived_csv_named_csv(self, client_clean: DataportalClient, addmockfile, tmp_path, mode):
        # pandas reads (compressed) tar archives with compression='tar', not with the outer codec
        src = tmp_path / 'inner.csv'
        src.write_bytes(self.df.to_csv(index=False).encode())
        archive = tmp_path / 'archive'
        with tarfile.open(archive, mode) as tf:
            tf.add(src, arcname='inner.csv')
        fileid = addmockfile('archived.csv', archive.read_bytes())

        client_clean.fromDataset(mockdsmetrics)
        assert client_clean.getData(fileid).shape == self.df.shape

    def test_gzip_csv_named_csv(self, client_clean: DataportalClient, addmockfile):
        content = gzip.compress(self.df.to_csv(index=False).encode())
        fileid = addmockfile('compressed.csv', content)

        client_clean.fromDataset(mockdsmetrics)
        df = client_clean.getData(fileid)
        assert df.shape == self.df.shape
        assert list(df['topic']) == list(self.df['topic'])
        assert [len(d) for d in client_clean.getData(fileid, entries=3, generator=True)] == [3, 3, 1]
        assert len(client_clean.getData(fileid, entries=-1)) == len(self.df)

    @pytest.mark.parametrize("compression", ['bz2', 'xz', 'zstd'])
    def test_compressed_json_lines_named_json(self, client_clean: DataportalClient, addmockfile, tmp_path, compression):
        path = tmp_path / f'written.{compression}'
        self.df.to_json(path, orient='records', lines=True, date_format='iso', compression=compression)
        fileid = addmockfile(f'compressed_{compression}.json', path.read_bytes())

        client_clean.fromDataset(mockdsmetrics)
        assert client_clean.getData(fileid).shape == self.df.shape
        assert client_clean.getData(fileid, entries=-1).shape == self.df.shape

    def test_parquet_still_drops_compression_with_warning(self, client_clean: DataportalClient, addmockfile, tmp_path, caplog):
        path = tmp_path / 'p.parquet'
        self.df.to_parquet(path)
        fileid = addmockfile('compressed_arg.parquet', path.read_bytes())

        client_clean.fromDataset(mockdsmetrics)
        with caplog.at_level(logging.WARNING, logger='dataportal.dataportalclient'):
            df = client_clean.getData(fileid, compression='bz2')
        pd.testing.assert_frame_equal(df, self.df)
        assert any('ignoring compression' in r.getMessage() for r in caplog.records)

    @pytest.mark.parametrize("head, expected", [
        (b'BZh91AY&SY', 'bz2'),
        (b'BZh,not,bz2', None),
        (b'BZh', None),
        (b'\x1f\x8b\x08', 'gzip'),
        (b'\xfd7zXZ\x00\x00', 'xz'),
        (b'\x28\xb5\x2f\xfd\x00', 'zstd'),
        (b'PK\x03\x04', 'zip'),
        (b'a,b,c\n1,2,3\n', None),
        (b'', None),
    ])
    def test_sniff_compression(self, tmp_path, head, expected):
        path = tmp_path / 'f'
        path.write_bytes(head)
        assert _sniffCompression(str(path)) == expected


def _response(status=200, payload=None):
    """A real requests.Response, so raise_for_status() raises an HTTPError carrying .response."""
    resp = requests.Response()
    resp.status_code = status
    resp._content = json.dumps(payload if payload is not None else {}).encode()
    return resp


_PAGE = {
    "data": [{"FileID": 1}],
    "pagination": {"total_records": 1, "next_page": {}, "current_page": {"index": 1}},
}


class TestChunkedListContract:
    URL = "http://chunked.test"
    PATH = "files"

    @pytest.fixture
    def get(self, mocker):
        """Replaces Session.get (overriding the autouse fixture); tests set .side_effect."""
        return mocker.patch("requests.Session.get")

    def make(self, **kwargs):
        return ChunkedList(self.URL, self.PATH, mocktoken, **kwargs)

    # --- __getitem__ ---
    # The contract is ONE request: the portal's single-file route returns datafiles and
    # extrafiles alike and accepts no extrafile parameter (both spellings answer 400), so
    # the old 404-retry with ?extrafile=true could only ever produce a 400, which the old
    # code swallowed -- returning None for a missing FileID and leaving the KeyError below
    # unreachable. These tests pin the one-request contract down.

    def test_missing_id_keyerror_contains_actual_id(self, get):
        get.side_effect = [_response(404)]
        with pytest.raises(KeyError) as e:
            self.make()[4242]
        assert "4242" in str(e.value)
        assert "{index}" not in str(e.value)

    def test_missing_id_does_not_retry_with_an_invalid_parameter(self, get):
        # The retry sent ?extrafile=true, which the portal rejects with 400. A second
        # request here means the dead fallback is back.
        get.side_effect = [_response(404)]
        with pytest.raises(KeyError):
            self.make()[4242]
        assert get.call_count == 1
        assert "extrafile" not in get.call_args.args[0]

    def test_non_404_raises_httperror_and_never_returns_none(self, get):
        # The old code let a non-404 fall off the end of __getitem__ and returned None.
        get.side_effect = [_response(500)]
        with pytest.raises(HTTPError) as e:
            self.make()[7]
        assert e.value.response.status_code == 500
        assert get.call_count == 1

    def test_datafile_is_returned_in_one_request(self, get):
        get.side_effect = [_response(200, {"FileID": 7})]
        assert self.make()[7] == {"FileID": 7}
        assert get.call_count == 1
        assert "extrafile" not in get.call_args.args[0]

    def test_extrafile_is_returned_by_the_same_route(self, get):
        # Verified against the portal: the plain route answers 200 for an extrafile id.
        get.side_effect = [_response(200, {"FileID": 7, "OriginName": "extrafile"})]
        assert self.make()[7] == {"FileID": 7, "OriginName": "extrafile"}
        assert get.call_count == 1

    # --- constructor ---

    def test_default_constructor_works_and_orders_by_fileid(self, get):
        get.side_effect = lambda *a, **k: _response(200, _PAGE)
        cl = ChunkedList(self.URL, self.PATH, mocktoken)
        assert cl.getCount() == 1
        assert "order_by=FileID" in get.call_args.args[0]

    def test_invalid_arguments_raise_valueerror(self):
        with pytest.raises(ValueError, match="orderBy must be one of"):
            self.make(orderBy="Bogus")
        with pytest.raises(ValueError, match="orderBy must be one of"):
            self.make(orderBy=None)
        with pytest.raises(ValueError, match="sort must be one of"):
            self.make(sort="sideways")

    # --- per-instance cache ---

    def test_instance_is_garbage_collected_after_cached_request(self, get):
        # Regression test for the class-level lru_cache leak: this test FAILS with the old
        # class-level cache (the cache, a GC root, keyed on and so retained every instance).
        get.side_effect = lambda *a, **k: _response(200, _PAGE)
        cl = self.make()
        cl.getCount()
        assert get.call_count == 1
        ref = weakref.ref(cl)
        del cl
        gc.collect()
        assert ref() is None

    def test_caches_are_isolated_between_instances(self, get):
        # Invariant, NOT a regression test: the old class-level cache already keyed on `self`,
        # so instances never shared entries -- measured, this passes on the pre-fix code too.
        # It is kept so the per-instance rewrite cannot accidentally introduce sharing.
        get.side_effect = lambda *a, **k: _response(200, _PAGE)
        first, second = self.make(), self.make()
        first.getCount()
        second.getCount()
        assert get.call_count == 2

    def test_pickle_round_trip_survives_the_per_instance_cache(self, get):
        # The cache is a live lru_cache wrapper in __dict__, which pickle cannot serialise.
        # Pickling worked before the cache moved onto the instance, so __getstate__/__setstate__
        # drop and rebuild it; without them this raises PicklingError.
        get.side_effect = lambda *a, **k: _response(200, _PAGE)
        cl = self.make()
        cl.getCount()
        restored = pickle.loads(pickle.dumps(cl))
        assert restored.getCount() == _PAGE["pagination"]["total_records"]
        # the rebuilt instance must be a fully working, registered, collectable one
        assert hasattr(restored._sendRequest, "cache_clear")
        assert restored in ChunkedList._instances
        ref = weakref.ref(restored)
        del restored
        gc.collect()
        assert ref() is None

    def test_same_request_on_one_instance_is_cached(self, get):
        get.side_effect = lambda *a, **k: _response(200, _PAGE)
        cl = self.make()
        cl._sendRequest("files", query={"a": 1})
        cl._sendRequest("files", query={"a": 1})
        assert get.call_count == 1

    def test_cache_clear_class_and_instance(self, get):
        get.side_effect = lambda *a, **k: _response(200, _PAGE)
        cl = self.make()
        other = self.make()
        for inst in (cl, other):
            inst._sendRequest("files", query={"a": 1})
        assert get.call_count == 2

        assert callable(ChunkedList._sendRequest.cache_clear)
        ChunkedList._sendRequest.cache_clear()
        for inst in (cl, other):
            inst._sendRequest("files", query={"a": 1})
        assert get.call_count == 4  # both live instances were cleared

        cl._sendRequest("files", query={"a": 1})
        assert get.call_count == 4  # cached again
        cl._sendRequest.cache_clear()
        cl._sendRequest("files", query={"a": 1})
        assert get.call_count == 5
        other._sendRequest("files", query={"a": 1})
        assert get.call_count == 5  # instance clear did not touch the other instance


# ---------------------------------------------------------------------------------------------------------------
# Authentication / authorisation failures: the user must be told what is wrong, not shown requests' internal error.
# The 401/403 bodies below are the portal's own (wcib middleware/jwt.js, session.js, apiScopeValidator.js); they
# are taken from that source, not from a live portal.
# ---------------------------------------------------------------------------------------------------------------

INTERNAL_URL = "http://dataportal-wcibapi-service.frontend.svc.cluster.local:3000/v1/validatetoken"

BODY_UNAUTHORIZED = "unauthorized user"
BODY_BLACKLISTED = "blacklisted user token"
BODY_NO_PERMISSIONS = "user alice has no permissions for this action on dataset with ID 2"
BODY_SCOPE_PATH = "Token does not have permission for this API path"
BODY_SCOPE_ACTION = "Token does not have permission for this action on dataset 2"
BODY_ROLE_DOWNGRADED = "Current role does not allow this action"


def _b64url(obj) -> str:
    return base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip("=")


def mintToken(**claims) -> str:
    """A JWT-shaped token (HS256 header, url-safe base64, padding stripped). The signature is junk: the client
    never verifies it, only the portal does. No `iat` unless given, so the expiry window falls back to its floor."""
    payload = {"user": {"username": "alice", "loggedIn": True, "groups": ["admins"]}, "uuid": "u-1"}
    payload.update(claims)
    return f"{_b64url({'alg': 'HS256', 'typ': 'JWT'})}.{_b64url(payload)}.c2lnbmF0dXJl"


def errResponse(status, body, url=INTERNAL_URL, reason="Unauthorized"):
    """A real requests.Response so raise_for_status() builds a real HTTPError carrying .response and .request, with a body to read."""
    resp = requests.Response()
    resp.status_code = status
    resp.url = url
    resp.reason = reason
    resp.request = requests.Request("GET", url).prepare()
    resp.raw = io.BytesIO(body.encode())
    return resp


@pytest.fixture
def realGet():
    """The routing mock installed by the autouse mocked_responses fixture, before a test replaces it."""
    return requests.Session.get.side_effect


@pytest.fixture
def connected(client_clean, realGet, mocker):
    """A client connected with the mock token, on a dataset, whose later requests the test can make fail."""
    client = client_clean.fromDataset(mockdserr)
    client.reconnectWaitTime = 0.1

    def failing(routeFails):
        """routeFails(url) -> a Response to return, or None to serve the request normally."""
        def get(url, headers=None, stream=False):
            failed = routeFails(url)
            if failed is not None:
                return failed
            return realGet(url, headers={"Authorization": "Bearer " + mocktoken}, stream=stream)
        return mocker.patch("requests.Session.get", side_effect=get)

    client.failing = failing
    return client


class TestTokenClaimsDecoding:
    def test_decodes_urlsafe_unpadded_payload(self):
        # '>>>???' encodes to '+' and '/' in standard base64, i.e. '-' and '_' url-safe, and the payload length is not a multiple of 4
        token = mintToken(marker=">>>???~~~")
        assert "=" not in token
        assert "+" not in token.split(".")[1]
        assert decodeTokenClaims(token)["marker"] == ">>>???~~~"
        assert decodeTokenClaims(token)["user"]["username"] == "alice"

    @pytest.mark.parametrize("token", [
        "asd123",                        # not a JWT at all
        "",
        "a.b",                           # two parts
        "a.b.c.d",                       # four parts
        "x.@@@@.y",                      # payload is not base64
        f"x.{_b64url('plainstring')}.y", # payload is JSON, but a string not an object
        f"x.{_b64url([1, 2])}.y",        # payload is JSON, but a list
        "x." + base64.urlsafe_b64encode(b"\xff\xfe not json").decode() + ".y",  # decodes, is not JSON
        "x." + base64.urlsafe_b64encode(b"\xff\xfe\xfd").decode() + ".y",       # not UTF-8
        "é.é.é",                         # not ASCII
    ])
    def test_non_jwt_gives_no_claims_and_no_exception(self, token):
        assert decodeTokenClaims(token) == {}
        assert checkTokenExpiry(token) is None  # must pass silently, the server decides


class TestPreflightExpiry:
    def test_expired_token_fails_before_any_request(self, mocker):
        get = mocker.patch("requests.Session.get", side_effect=AssertionError("no network call may be made to diagnose an expired token"))
        exp = int(time.time() - 3.2 * 3600)
        with pytest.raises(TokenExpiredError) as e:
            DataportalClient(mintToken(exp=exp))
        get.assert_not_called()

        text = str(e.value)
        assert f"expired at {datetime.fromtimestamp(exp, tz=UTC).strftime('%Y-%m-%dT%H:%M:%SZ')}" in text
        assert "3.2 hours ago" in text
        assert "Get a new token" in text
        assert "svc.cluster.local" not in text

    @pytest.mark.parametrize(("ago", "expected"), [(30, "30 seconds ago"), (10 * 60, "10.0 minutes ago"), (5 * 86400, "5.0 days ago")])
    def test_how_long_ago_scales(self, ago, expected):
        with pytest.raises(TokenExpiredError, match=re.escape(expected)):
            checkTokenExpiry(mintToken(exp=time.time() - ago))

    def test_expired_token_error_is_still_an_httperror(self):
        # callers that catch HTTPError keep working
        with pytest.raises(HTTPError):
            checkTokenExpiry(mintToken(exp=time.time() - 60))

    def test_unexpired_token_connects(self, realGet, mocker):
        mocker.patch("requests.Session.get", side_effect=lambda url, headers=None, stream=False: realGet(url, headers={"Authorization": "Bearer " + mocktoken}, stream=stream))
        DataportalClient(mintToken(exp=time.time() + 3600))

    def test_jwt_without_exp_skips_the_check(self):
        assert checkTokenExpiry(mintToken()) is None

    @pytest.mark.parametrize("exp", ["soon", None, True, [1], float("nan"), float("inf"), 10**400])
    def test_unusable_exp_skips_the_check(self, exp):
        assert checkTokenExpiry(mintToken(exp=exp)) is None

    def test_huge_exp_does_not_crash_formatting(self):
        # far in the past/future values that datetime cannot represent must not turn the diagnosis into an OverflowError
        assert checkTokenExpiry(mintToken(exp=10**15)) is None  # year ~31 million: warns not, formats not
        with pytest.raises(TokenExpiredError):
            checkTokenExpiry(mintToken(exp=-(10**15)))

    def test_non_jwt_token_connects_as_before(self, caplog):
        with caplog.at_level(logging.INFO):
            DataportalClient(mocktoken)
        assert "Connection OK" in caplog.text


@pytest.fixture
def acceptAnyToken(realGet, mocker):
    """Session.get served by the routing mock whatever token the client sends."""
    return mocker.patch("requests.Session.get", side_effect=lambda url, headers=None, stream=False: realGet(url, headers={"Authorization": "Bearer " + mocktoken}, stream=stream))


class TestNearExpiryWarning:
    """A still-valid token close to expiring is warned about (log only, never an exception)."""

    NOW = 1_800_000_000.0

    def token(self, remaining, lifetime=None):
        claims = {"exp": self.NOW + remaining}
        if lifetime is not None:
            claims["iat"] = self.NOW + remaining - lifetime
        return mintToken(**claims)

    @pytest.mark.parametrize(("lifetime", "window"), [
        (None, 300),                  # no iat: the floor
        (20 * 60, 300),               # 20 minutes: 10% is 2 min, the 5 min floor wins
        (60 * 60, 360),               # 1 hour: 10%
        (24 * 3600, 8640),            # 1 day: 10% = 2.4 h
        (1095 * 86400, 86400),        # MQTT setup token: 10% would be 109 days, capped at 1 day
    ])
    def test_window_scales_with_the_tokens_own_lifetime(self, lifetime, window):
        claims = decodeTokenClaims(self.token(10, lifetime))
        assert expiryWarnWindow(claims) == window

    def test_window_is_overridable(self):
        claims = decodeTokenClaims(self.token(10, 3600))
        assert expiryWarnWindow(claims, 45) == 45
        assert expiryWarnWindow(claims, 0) == 0
        assert expiryWarnWindow(claims, -5) == 0

    @pytest.mark.parametrize("lifetime", [None, 1200, 3600])
    def test_boundary(self, lifetime):
        window = expiryWarnWindow(decodeTokenClaims(self.token(10, lifetime)))
        # just inside the window: warns, and does not raise
        inside = checkTokenExpiry(self.token(window - 1, lifetime), now=self.NOW)
        assert inside is not None
        assert inside.startswith("dataportal token expires at ")
        # just outside, and well outside: silent
        assert checkTokenExpiry(self.token(window + 1, lifetime), now=self.NOW) is None
        assert checkTokenExpiry(self.token(window * 50, lifetime), now=self.NOW) is None
        # at the very instant of exp, and past it: the token is expired and that raises
        with pytest.raises(TokenExpiredError):
            checkTokenExpiry(self.token(0, lifetime), now=self.NOW)
        with pytest.raises(TokenExpiredError):
            checkTokenExpiry(self.token(-1, lifetime), now=self.NOW)

    def test_message_names_the_instant_and_the_time_left(self):
        text = checkTokenExpiry(self.token(252), now=self.NOW)
        instant = datetime.fromtimestamp(self.NOW + 252, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        assert text == f"dataportal token expires at {instant} (in 4.2 minutes); long downloads may fail part-way - consider a fresh token"

    def test_warning_function_is_silent_when_it_cannot_tell(self):
        for token in ("asd123", mintToken(), mintToken(exp="soon"), self.token(-100)):
            assert expiryWarning(token, now=self.NOW) is None

    def test_connect_warns_and_proceeds(self, acceptAnyToken, caplog):
        with caplog.at_level(logging.INFO):
            client = DataportalClient(mintToken(exp=time.time() + 120))
        assert isinstance(client, DataportalClient)
        warned = [r for r in caplog.records if r.levelno == logging.WARNING and "token expires at" in r.getMessage()]
        assert len(warned) == 1
        assert "consider a fresh token" in warned[0].getMessage()
        assert "Connection OK" in caplog.text

    def test_connect_is_silent_for_a_comfortably_valid_token(self, acceptAnyToken, caplog):
        with caplog.at_level(logging.INFO):
            DataportalClient(mintToken(exp=time.time() + 3600))
        assert "token expires at" not in caplog.text
        assert "Connection OK" in caplog.text

    def test_connect_with_an_expired_token_raises(self, acceptAnyToken):
        with pytest.raises(TokenExpiredError):
            DataportalClient(mintToken(exp=time.time() - 1))

    def test_connect_threshold_follows_the_tokens_lifetime(self, acceptAnyToken, caplog):
        # 20-minute token with 8 minutes left: a flat 1 hour window would warn, the lifetime-based one does not
        now = time.time()
        with caplog.at_level(logging.INFO):
            DataportalClient(mintToken(iat=now - 12 * 60, exp=now + 8 * 60))
        assert "token expires at" not in caplog.text

    @pytest.mark.parametrize(("remaining", "override", "warns"), [(120, 60, False), (120, 0, False), (3600, 7200, True)])
    def test_connect_threshold_is_overridable(self, acceptAnyToken, caplog, remaining, override, warns):
        with caplog.at_level(logging.INFO):
            DataportalClient(mintToken(exp=time.time() + remaining), warnBeforeExpiry=override)
        assert ("token expires at" in caplog.text) is warns

    def test_download_warns_before_streaming_without_an_extra_request(self, connected, tmp_path, caplog):
        file = next(f for f in mockfiles[mockdatasetids[mockdserr]] if f["OriginName"] == "thisworks")
        connected.token = mintToken(exp=time.time() + 90)
        gets = connected.failing(lambda url: None)
        with caplog.at_level(logging.INFO):
            connected._downloadFile(file["FileID"], str(tmp_path))
        warned = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING and "token expires at" in r.getMessage()]
        assert len(warned) == 1
        assert f"about to download file {file['FileID']}" in warned[0]
        # the check is local: the only request is the download itself
        assert gets.call_count == 1
        assert gets.call_args.args[0].endswith("/content?extrafile=false")
        assert (tmp_path / file["MFileName"]).stat().st_size > 0

    def test_download_warns_once_even_when_it_retries(self, connected, tmp_path, caplog):
        file = next(f for f in mockfiles[mockdatasetids[mockdserr]] if f["OriginName"] == "ChunkedEncodingError1")
        connected.token = mintToken(exp=time.time() + 90)
        connected.failing(lambda url: None)
        with caplog.at_level(logging.INFO):
            connected._downloadFile(file["FileID"], str(tmp_path))
        assert caplog.text.count("token expires at") == 1

    def test_download_is_silent_for_a_comfortably_valid_token(self, connected, tmp_path, caplog):
        file = next(f for f in mockfiles[mockdatasetids[mockdserr]] if f["OriginName"] == "thisworks")
        connected.token = mintToken(exp=time.time() + 86400)
        connected.failing(lambda url: None)
        with caplog.at_level(logging.INFO):
            connected._downloadFile(file["FileID"], str(tmp_path))
        assert "token expires at" not in caplog.text

    def test_download_never_refuses_on_an_unverified_expiry(self, connected, tmp_path, caplog):
        # the claim says expired, the portal (mock) accepts: the download goes ahead, no exception, no warning
        file = next(f for f in mockfiles[mockdatasetids[mockdserr]] if f["OriginName"] == "thisworks")
        connected.token = mintToken(exp=time.time() - 60)
        connected.failing(lambda url: None)
        with caplog.at_level(logging.INFO):
            connected._downloadFile(file["FileID"], str(tmp_path))
        assert (tmp_path / file["MFileName"]).stat().st_size > 0
        assert "token expires at" not in caplog.text


class TestAuthErrorsAtConnect:
    """The six request sites all go through _sendRequest, so connect shows the same translation."""

    def connectWith(self, mocker, token, status, body):
        mocker.patch("requests.Session.get", side_effect=lambda url, headers=None, stream=False: errResponse(status, body))
        return DataportalClient(token)

    def test_401_not_expired_is_a_rejected_token(self, mocker):
        with pytest.raises(TokenRejectedError) as e:
            self.connectWith(mocker, mintToken(exp=time.time() + 3600), 401, BODY_UNAUTHORIZED)
        text = str(e.value)
        assert "not expired" in text
        assert "different environment" in text
        assert "staging" in text
        assert "truncated" in text
        assert "svc.cluster.local" not in text
        assert "validatetoken" not in text

    def test_non_jwt_token_rejected_by_server(self, mocker):
        with pytest.raises(TokenRejectedError, match="not expired"):
            self.connectWith(mocker, "asd123", 401, BODY_UNAUTHORIZED)

    def test_jwt_without_exp_rejected_by_server(self, mocker):
        with pytest.raises(TokenRejectedError, match="not expired"):
            self.connectWith(mocker, mintToken(), 401, BODY_UNAUTHORIZED)

    def test_original_error_is_chained_for_debugging(self, mocker):
        with pytest.raises(TokenRejectedError) as e:
            self.connectWith(mocker, mintToken(), 401, BODY_UNAUTHORIZED)
        cause = e.value.__cause__
        assert isinstance(cause, HTTPError)
        # the detail is kept in the chain, with the host removed and the path (which call failed) kept
        assert "401 Client Error: Unauthorized for url: /v1/validatetoken" == str(cause)
        assert INTERNAL_URL not in str(e.value)
        assert e.value.response.status_code == 401
        assert e.value.response is cause.response

    def test_translated_response_is_closed(self, mocker):
        resp = errResponse(401, BODY_UNAUTHORIZED)
        close = mocker.spy(resp, "close")
        mocker.patch("requests.Session.get", return_value=resp)
        with pytest.raises(TokenRejectedError):
            DataportalClient(mintToken())
        close.assert_called_once()

    @pytest.mark.parametrize("status", [400, 404, 500, 502])
    def test_other_http_errors_are_raised_as_requests_raised_them(self, mocker, status):
        with pytest.raises(HTTPError) as e:
            self.connectWith(mocker, mintToken(), status, "whatever")
        assert not isinstance(e.value, DataportalAuthError)

    def test_unrecognised_403_is_raised_untranslated(self, mocker):
        with pytest.raises(HTTPError) as e:
            self.connectWith(mocker, mintToken(), 403, "Forbidden by something unrelated")
        assert not isinstance(e.value, DataportalAuthError)

    def test_error_without_a_response_is_raised_untranslated(self):
        # e.g. the mocks elsewhere in this file raise a bare HTTPError
        err = HTTPError("boom")
        assert explainHTTPError(err, mocktoken, "v1/x") is None


class TestMidSessionAuthErrors:
    """A token can expire or be revoked while the notebook is running. getData, listFiles and the streaming
    download must give the same messages as connect."""

    def expire(self, client, ago=3600 * 3.2):
        client.token = mintToken(exp=time.time() - ago)

    def test_getdata_after_expiry(self, connected):
        file = next(f for f in mockfiles[mockdatasetids[mockdserr]] if not f["ExtraFile"])
        self.expire(connected)
        connected.failing(lambda url: errResponse(401, BODY_UNAUTHORIZED))
        with pytest.raises(TokenExpiredError, match=r"expired at .* \(3\.2 hours ago") as e:
            connected.getData(file["FileID"])
        assert "svc.cluster.local" not in str(e.value)
        assert connected.state.file is None

    def test_listfiles_after_expiry(self, connected):
        self.expire(connected)
        connected.failing(lambda url: errResponse(401, BODY_UNAUTHORIZED))
        with pytest.raises(TokenExpiredError, match="3.2 hours ago"):
            next(iter(connected.listFiles()))

    def test_listfiles_names_the_dataset_on_no_access(self, connected):
        connected.failing(lambda url: errResponse(401, BODY_NO_PERMISSIONS))
        with pytest.raises(DatasetAccessDeniedError, match=r"'mockerr' \(ID 2\)"):
            next(iter(connected.listFiles()))

    def test_listfiles_item_lookup_after_expiry(self, connected):
        files = connected.listFiles()
        self.expire(connected)
        files._token = connected.token
        connected.failing(lambda url: errResponse(401, BODY_UNAUTHORIZED))
        with pytest.raises(TokenExpiredError):
            files[mockfiles[mockdatasetids[mockdserr]][0]["FileID"]]

    def test_fromdataset_after_expiry(self, connected):
        self.expire(connected)
        connected.failing(lambda url: errResponse(401, BODY_UNAUTHORIZED))
        with pytest.raises(TokenExpiredError):
            connected.fromDataset(mockdslogs)

    def test_streaming_download_after_expiry(self, connected, tmp_path, mocker):
        file = next(f for f in mockfiles[mockdatasetids[mockdserr]] if f["OriginName"] == "thisworks")
        self.expire(connected)
        resp = errResponse(401, BODY_UNAUTHORIZED)
        close = mocker.spy(resp, "close")
        gets = connected.failing(lambda url: resp if url.endswith("/content?extrafile=false") else None)

        with pytest.raises(TokenExpiredError, match="3.2 hours ago") as e:
            connected._downloadFile(file["FileID"], str(tmp_path))

        assert "svc.cluster.local" not in str(e.value)
        # raised before the `with` in _downloadFile is entered, so _sendRequest itself must release the connection
        close.assert_called_once()
        # an authentication failure is not a dropped connection: it must not be retried
        contentCalls = [c for c in gets.call_args_list if c.args[0].endswith("/content?extrafile=false")]
        assert len(contentCalls) == 1
        assert list(tmp_path.iterdir()) == []

    def test_streaming_download_via_getdata_leaves_no_state(self, connected):
        file = next(f for f in mockfiles[mockdatasetids[mockdserr]] if f["OriginName"] == "thisworks")
        self.expire(connected)
        connected.failing(lambda url: errResponse(401, BODY_UNAUTHORIZED) if url.endswith("/content?extrafile=false") else None)
        with pytest.raises(TokenExpiredError):
            connected.getData(file["FileID"])
        assert connected.state.file is None

    def test_streaming_download_revoked(self, connected, tmp_path):
        file = next(f for f in mockfiles[mockdatasetids[mockdserr]] if f["OriginName"] == "thisworks")
        connected.failing(lambda url: errResponse(401, BODY_BLACKLISTED) if url.endswith("/content?extrafile=false") else None)
        with pytest.raises(TokenRevokedError, match="revoked"):
            connected._downloadFile(file["FileID"], str(tmp_path))

    def test_getextrafiles_reports_the_clear_error_per_file(self, connected, tmp_path):
        connected.fromDataset(mockdsmetrics)
        extra = next(f for f in mockfiles[mockdatasetids[mockdsmetrics]] if f["ExtraFile"])
        self.expire(connected)
        connected.failing(lambda url: errResponse(401, BODY_UNAUTHORIZED) if url.endswith("/content?extrafile=true") else None)
        ids, failures = connected.getExtraFiles(extra["FileID"])
        assert ids == []
        assert isinstance(failures[extra["FileID"]], TokenExpiredError)

    def test_forged_expired_claim_does_not_stop_a_request_the_server_accepts(self, connected):
        # Claims are not trusted: a token whose payload says "expired" is still sent, and if the portal accepts it the
        # request succeeds. Only the pre-flight check at connect uses exp, and only to improve a message.
        file = next(f for f in mockfiles[mockdatasetids[mockdserr]] if f["OriginName"] == "thisworks")
        self.expire(connected)
        connected.failing(lambda url: None)
        assert connected.getData(file["FileID"]) is not None

    def test_claims_never_override_the_servers_answer(self, connected):
        # forged groups / far-future exp / admin everything: the 401 is still raised, and is not called "expired"
        connected.token = mintToken(exp=time.time() + 10**9, user={"username": "root", "groups": ["admins", "superuser"]})
        connected.failing(lambda url: errResponse(401, BODY_UNAUTHORIZED))
        with pytest.raises(TokenRejectedError):
            connected.listFiles().getCount()

    def test_401_without_exp_is_not_called_expired(self, connected):
        connected.token = mintToken()
        connected.failing(lambda url: errResponse(401, BODY_UNAUTHORIZED))
        with pytest.raises(TokenRejectedError):
            connected.fromDataset(mockdslogs)


class TestAuthMessages:
    """Which cause is named for which body. The server's body wins over the local expiry."""

    def explain(self, status, body, token=None, path="v1/dataset/2/files", datasetName="mockerr", datasetId=2):
        err = HTTPError(response=errResponse(status, body))
        return explainHTTPError(err, token if token is not None else mintToken(), path, datasetName, datasetId)

    def test_no_permissions_names_user_dataset_and_next_step(self):
        e = self.explain(401, BODY_NO_PERMISSIONS)
        assert isinstance(e, DatasetAccessDeniedError)
        text = str(e)
        assert "user 'alice'" in text
        assert "'mockerr' (ID 2)" in text
        assert "Ask the owner of the dataset" in text
        assert "valid" in text
        assert "expired" not in text
        assert "svc.cluster.local" not in text

    def test_no_permissions_beats_an_expired_claim(self):
        # a token that is expired by its claim but whose request reached the permission check was accepted by the server
        e = self.explain(401, BODY_NO_PERMISSIONS, token=mintToken(exp=time.time() - 99999))
        assert isinstance(e, DatasetAccessDeniedError)

    def test_no_permissions_prefers_the_servers_username_over_the_claim(self):
        e = self.explain(401, "user bob has no permissions for this action on dataset with ID 2", token=mintToken())
        assert "user 'bob'" in str(e)

    def test_no_permissions_falls_back_to_the_claim_username(self):
        e = self.explain(401, "has no permissions", token=mintToken())
        assert "user 'alice'" in str(e)

    def test_no_permissions_on_a_non_jwt_token_still_explains(self):
        e = self.explain(401, "user carol has no permissions for this action on dataset with ID 7", token="asd123", datasetName="mockerr", datasetId=2)
        assert "user 'carol'" in str(e)
        assert "ID 7" in str(e)
        assert "mockerr" not in str(e)  # that name belongs to dataset 2, the server said 7

    def test_blacklisted_means_revoked(self):
        e = self.explain(401, BODY_BLACKLISTED)
        assert isinstance(e, TokenRevokedError)
        assert "revoked" in str(e)
        assert "Get a new token" in str(e)

    def test_blacklisted_beats_an_expired_claim(self):
        assert isinstance(self.explain(401, BODY_BLACKLISTED, token=mintToken(exp=time.time() - 99999)), TokenRevokedError)

    def test_unauthorized_with_past_exp_is_expired(self):
        e = self.explain(401, BODY_UNAUTHORIZED, token=mintToken(exp=time.time() - 7200))
        assert isinstance(e, TokenExpiredError)
        assert "2.0 hours ago" in str(e)

    def test_unauthorized_with_future_exp_is_rejected_not_expired(self):
        e = self.explain(401, BODY_UNAUTHORIZED, token=mintToken(exp=time.time() + 7200))
        assert isinstance(e, TokenRejectedError)
        assert "expired at" not in str(e)

    def test_body_matching_is_case_insensitive(self):
        assert isinstance(self.explain(401, "BLACKLISTED USER TOKEN"), TokenRevokedError)

    def test_no_dataset_known_still_gives_a_message(self):
        e = self.explain(401, "has no permissions", path="v1/dataset", datasetName=None, datasetId=None)
        assert isinstance(e, DatasetAccessDeniedError)
        assert "the dataset you were accessing" in str(e)

    def test_no_message_recommends_a_method_that_does_not_exist(self):
        assert not hasattr(DataportalClient, "listDatasets")
        assert "listDatasets" not in str(self.explain(401, BODY_NO_PERMISSIONS))


class TestScopedTokens:
    """apiScopeValidator.js answers 403 'Token does not have permission for this API path' when a scoped token's
    apiScope has no entry covering the path and method. session.js checkRole answers 401 'Token does not have
    permission for this action on dataset N' for the older `permissions` claim, and 403 'Current role does not
    allow this action' when the user's role was reduced after the token was issued."""

    SCOPE = [{"path": "/v1/dataset/3/*", "acc": "r"}]

    def explain(self, status, body, token, path="v1/dataset/2/files"):
        return explainHTTPError(HTTPError(response=errResponse(status, body)), token, path, "mockerr", 2)

    def test_scope_denial_403_says_what_the_token_is_scoped_to(self):
        e = self.explain(403, BODY_SCOPE_PATH, mintToken(apiScope=self.SCOPE))
        assert isinstance(e, TokenScopeError)
        text = str(e)
        assert "/v1/dataset/3/* (r)" in text
        assert "GET /v1/dataset/2/files" in text
        assert "outside" in text
        assert "svc.cluster.local" not in text

    def test_empty_scope_is_described_as_allowing_nothing(self):
        e = self.explain(403, BODY_SCOPE_PATH, mintToken(apiScope=[]))
        assert isinstance(e, TokenScopeError)
        assert "allows no API access" in str(e)

    def test_scope_denial_without_a_readable_scope_still_explains(self):
        e = self.explain(403, BODY_SCOPE_PATH, "asd123")
        assert isinstance(e, TokenScopeError)
        assert "scoped" in str(e)

    def test_permissions_claim_denial_is_a_401_scope_error(self):
        e = self.explain(401, BODY_SCOPE_ACTION, mintToken(permissions={"dataset": 2, "actions": ["upload_files"]}))
        assert isinstance(e, TokenScopeError)
        assert "dataset 2, actions ['upload_files']" in str(e)

    def test_scope_denial_beats_an_expired_claim(self):
        assert isinstance(self.explain(403, BODY_SCOPE_PATH, mintToken(apiScope=self.SCOPE, exp=time.time() - 99999)), TokenScopeError)

    def test_role_downgraded_403(self):
        e = self.explain(403, BODY_ROLE_DOWNGRADED, mintToken())
        assert isinstance(e, DatasetAccessDeniedError)
        assert "no longer allows" in str(e)

    def test_malformed_apiscope_claim_does_not_crash_the_explanation(self):
        for scope in ("x", 5, [None, 3, {"nopath": 1}], {"path": "/v1"}):
            e = self.explain(403, BODY_SCOPE_PATH, mintToken(apiScope=scope))
            assert isinstance(e, TokenScopeError)

    def test_a_scoped_token_outside_its_scope_end_to_end(self, connected):
        connected.token = mintToken(apiScope=self.SCOPE)
        connected.failing(lambda url: errResponse(403, BODY_SCOPE_PATH))
        with pytest.raises(TokenScopeError, match=r"/v1/dataset/3/\* \(r\)"):
            connected.listFiles().getCount()


# ---------------------------------------------------------------------------------------------------------------
# The portal's host must not reach the user, the path must; and nothing may be hidden: the original exception stays
# in the chain, and is itself clean.
# ---------------------------------------------------------------------------------------------------------------

CLUSTER_HOST = "dataportal-wcibapi-service.frontend.svc.cluster.local"
CLUSTER_BASE = f"http://{CLUSTER_HOST}:3000"


def chainText(exc) -> str:
    """Every message in the exception chain, as it would be printed (no source lines)."""
    parts, seen = [], set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        parts.append(f"{type(exc).__name__}: {exc}")
        exc = exc.__cause__ or exc.__context__
    return "\n".join(parts)


def assertHostFree(text):
    for forbidden in (CLUSTER_HOST, "http://", "https://", ":3000", "localhost", "ingress.example.org", "10.1.2.3"):
        assert forbidden not in text, f"{forbidden!r} leaked into {text!r}"


def tracebackFiles(exc):
    tb, files = exc.__traceback__, []
    while tb is not None:
        files.append(tb.tb_frame.f_code.co_filename)
        tb = tb.tb_next
    return files


# (status, body, token, expected type, expected phrase)
TRANSLATED_CASES = {
    "expired": (401, BODY_UNAUTHORIZED, lambda: mintToken(exp=time.time() - 7200), TokenExpiredError),
    "revoked": (401, BODY_BLACKLISTED, mintToken, TokenRevokedError),
    "rejected": (401, BODY_UNAUTHORIZED, mintToken, TokenRejectedError),
    "dataset-denied": (401, BODY_NO_PERMISSIONS, mintToken, DatasetAccessDeniedError),
    "scope": (403, BODY_SCOPE_PATH, lambda: mintToken(apiScope=[{"path": "/v1/dataset/3/*", "acc": "r"}]), TokenScopeError),
    "role-reduced": (403, BODY_ROLE_DOWNGRADED, mintToken, DatasetAccessDeniedError),
}


class TestRedactUrls:
    @pytest.mark.parametrize(("text", "expected"), [
        (f"401 Client Error: Unauthorized for url: {CLUSTER_BASE}/v1/validatetoken",
         "401 Client Error: Unauthorized for url: /v1/validatetoken"),
        (f"404 Client Error: Not Found for url: {CLUSTER_BASE}/v1/dataset/2/files/9/content?extrafile=false",
         "404 Client Error: Not Found for url: /v1/dataset/2/files/9/content?extrafile=false"),
        ("x for url: http://localhost:8080/v1/version", "x for url: /v1/version"),
        ("x for url: https://ingress.example.org/v1/version", "x for url: /v1/version"),
        ("x for url: http://10.1.2.3:3000/v1/version?a=1&b=2", "x for url: /v1/version?a=1&b=2"),
        ("x for url: http://[::1]:8080/v1/version", "x for url: /v1/version"),
        ("x for url: https://user:secret@ingress.example.org/v1/version", "x for url: /v1/version"),  # credentials go too
        ("x for url: https://ingress.example.org", "x for url: /"),                                   # no path at all
        ("x for url: https://ingress.example.org/v1/dataset#frag", "x for url: /v1/dataset"),
        ("failed (http://localhost:8080/v1/a), then http://10.1.2.3/v1/b.", "failed (/v1/a), then /v1/b."),  # several, and punctuation
        ("no url in this message", "no url in this message"),
        ("", ""),
        ("file:///tmp/x", "file:///tmp/x"),                                                           # no host to hide
        ("x http://[unbalanced/v1", "x <url>"),                                                       # unparsable: hide, do not show
    ])
    def test_reduces_every_url_to_path_and_query(self, text, expected):
        assert autherrors.redactUrls(text) == expected

    def test_sanitise_error_is_in_place(self):
        err = HTTPError(f"401 for url: {CLUSTER_BASE}/v1/x", response=errResponse(401, ""))
        tb_before = err.__traceback__
        same = autherrors.sanitiseError(err)
        assert same is err
        assert str(err) == "401 for url: /v1/x"
        assert err.__traceback__ is tb_before
        assert err.response.status_code == 401

    def test_non_string_args_are_left_alone(self):
        marker = object()
        err = autherrors.sanitiseError(ValueError(f"{CLUSTER_BASE}/v1/x", marker, 3))
        assert err.args == ("/v1/x", marker, 3)


class TestHostRedactedEverywhere:
    @pytest.mark.parametrize("case", TRANSLATED_CASES)
    def test_translated_error_keeps_a_clean_chain(self, case):
        status, body, token, expected = TRANSLATED_CASES[case]
        resp = errResponse(status, body, url=f"{CLUSTER_BASE}/v1/validatetoken")
        with pytest.raises(expected) as e:
            raiseForStatus(resp, token(), "v1/validatetoken", "mockerr", 2)

        # the chain is intact: never hidden
        cause = e.value.__cause__
        assert cause is not None
        assert isinstance(cause, HTTPError)
        # the cause's OWN message is clean but still says which call failed
        assert str(cause) == f"{status} Client Error: Unauthorized for url: /v1/validatetoken"
        assertHostFree(str(cause))
        # what the user sees: the translated message and the whole chain
        assertHostFree(str(e.value))
        assertHostFree(chainText(e.value))
        assert "/v1/validatetoken" in chainText(e.value)
        # callers that read the response keep working
        assert e.value.response.status_code == status
        assert e.value.response is cause.response is resp

    @pytest.mark.parametrize("case", TRANSLATED_CASES)
    def test_the_printed_traceback_shows_the_whole_chain_without_the_host(self, case):
        import traceback

        status, body, token, expected = TRANSLATED_CASES[case]
        resp = errResponse(status, body, url=f"{CLUSTER_BASE}/v1/validatetoken")
        with pytest.raises(expected) as e:
            raiseForStatus(resp, token(), "v1/validatetoken", "mockerr", 2)
        printed = "".join(traceback.format_exception(e.value))
        assert "The above exception was the direct cause of the following exception" in printed
        assert "requests.exceptions.HTTPError" in printed
        assert "/v1/validatetoken" in printed
        assertHostFree(printed)

    def test_chained_cause_is_the_original_exception_not_a_copy(self):
        resp = errResponse(401, BODY_UNAUTHORIZED, url=f"{CLUSTER_BASE}/v1/validatetoken")
        with pytest.raises(TokenRejectedError) as e:
            raiseForStatus(resp, mintToken(), "v1/validatetoken")
        cause = e.value.__cause__
        # it was raised by requests, and its traceback still starts there (a copy would not have this)
        assert any(f.endswith("requests/models.py") for f in tracebackFiles(cause))
        # .request is untouched: only the rendered message changed
        assert cause.request is resp.request
        assert cause.request.url == f"{CLUSTER_BASE}/v1/validatetoken"
        # `raise ... from err` (never `from None`): the cause is attached and is what Python prints as the direct cause
        assert e.value.__cause__ is cause

    @pytest.mark.parametrize(("status", "reason"), [(404, "Not Found"), (500, "Internal Server Error"), (502, "Bad Gateway"), (403, "Forbidden")])
    def test_untranslated_errors_keep_type_and_status_but_lose_the_host(self, status, reason):
        resp = errResponse(status, "Forbidden by something unrelated", url=f"{CLUSTER_BASE}/v1/dataset/2/files/9/content?extrafile=false", reason=reason)
        with pytest.raises(HTTPError) as e:
            raiseForStatus(resp, mintToken(), "v1/dataset/2/files/9/content")
        assert type(e.value) is HTTPError  # not translated, not wrapped
        assert e.value.response is resp
        assert e.value.response.status_code == status
        kind = "Client" if status < 500 else "Server"
        # the path AND the query string survive
        assert str(e.value) == f"{status} {kind} Error: {reason} for url: /v1/dataset/2/files/9/content?extrafile=false"
        assertHostFree(str(e.value))

    @pytest.mark.parametrize("base", ["http://localhost:8080", "https://ingress.example.org", "http://10.1.2.3:3000", CLUSTER_BASE])
    @pytest.mark.parametrize("status", [401, 404])
    def test_any_base_url_is_redacted(self, base, status):
        resp = errResponse(status, BODY_UNAUTHORIZED, url=f"{base}/v1/dataset?x=1")
        with pytest.raises(HTTPError) as e:
            raiseForStatus(resp, mintToken(), "v1/dataset")
        text = chainText(e.value)
        assertHostFree(text)
        assert "/v1/dataset?x=1" in text

    def test_through_the_client_at_connect(self, mocker):
        mocker.patch("requests.Session.get", side_effect=lambda url, headers=None, stream=False: errResponse(401, BODY_UNAUTHORIZED, url=f"{CLUSTER_BASE}/v1/validatetoken"))
        with pytest.raises(TokenRejectedError) as e:
            DataportalClient(mintToken())
        text = chainText(e.value)
        assert "/v1/validatetoken" in text
        assertHostFree(text)
        assert e.value.__cause__ is not None
        assert e.value.response.status_code == 401

    def test_through_the_client_for_a_missing_file(self, connected):
        connected.failing(lambda url: errResponse(404, "", url=f"{CLUSTER_BASE}/v1/dataset/2/files/999", reason="Not Found") if url.endswith("/files/999") else None)
        with pytest.raises(ValueError, match="could not be found") as e:
            connected.getData(999)
        # the ValueError is chained to the original HTTPError, and that is clean
        assert isinstance(e.value.__cause__, HTTPError)
        assert str(e.value.__cause__) == "404 Client Error: Not Found for url: /v1/dataset/2/files/999"
        assertHostFree(chainText(e.value))

    def test_through_the_client_for_a_server_error_on_listing(self, connected):
        connected.failing(lambda url: errResponse(500, "", url=f"{CLUSTER_BASE}/v1/dataset/2/files?limit=500", reason="Internal Server Error") if "/files?" in url else None)
        with pytest.raises(HTTPError) as e:
            next(iter(connected.listFiles()))
        assert type(e.value) is HTTPError
        assert e.value.response.status_code == 500
        assert str(e.value) == "500 Server Error: Internal Server Error for url: /v1/dataset/2/files?limit=500"

    def test_chunkedlist_item_lookup_keeps_its_keyerror_and_a_clean_cause(self, connected):
        files = connected.listFiles()
        connected.failing(lambda url: errResponse(404, "", url=f"{CLUSTER_BASE}/v1/dataset/2/files/12345", reason="Not Found") if url.endswith("/files/12345") else None)
        with pytest.raises(KeyError) as e:
            files[12345]
        assert isinstance(e.value.__cause__, HTTPError)
        assertHostFree(chainText(e.value))
        assert "/v1/dataset/2/files/12345" in chainText(e.value)

    def test_getextrafiles_still_reads_status_from_the_raised_exceptions(self, connected):
        connected.fromDataset(mockdsmetrics)
        ids = [900, 901, 902]  # the failing() routes below intercept these requests, whatever the mock serves
        responses = {
            ids[0]: errResponse(404, "", url=f"{CLUSTER_BASE}/v1/dataset/0/files/{ids[0]}/content?extrafile=true", reason="Not Found"),
            ids[1]: errResponse(500, "", url=f"{CLUSTER_BASE}/v1/dataset/0/files/{ids[1]}/content?extrafile=true", reason="Internal Server Error"),
            ids[2]: errResponse(401, BODY_UNAUTHORIZED, url=f"{CLUSTER_BASE}/v1/dataset/0/files/{ids[2]}/content?extrafile=true"),
        }
        connected.token = mintToken(exp=time.time() - 60)
        connected.failing(lambda url: next((r for i, r in responses.items() if url.endswith(f"/files/{i}/content?extrafile=true")), None))

        done, failures = connected.getExtraFiles(ids)

        assert done == []
        # 404 keeps its existing translation to a ValueError
        assert isinstance(failures[ids[0]], ValueError)
        # 500 is the original HTTPError: status readable, URL sanitised, query kept
        assert type(failures[ids[1]]) is HTTPError
        assert failures[ids[1]].response.status_code == 500
        assert str(failures[ids[1]]) == f"500 Server Error: Internal Server Error for url: /v1/dataset/0/files/{ids[1]}/content?extrafile=true"
        # 401 is the translated error, whose cause is clean
        assert isinstance(failures[ids[2]], TokenExpiredError)
        assert failures[ids[2]].response.status_code == 401
        assertHostFree(chainText(failures[ids[2]]))
        assert "/content?extrafile=true" in chainText(failures[ids[2]])
