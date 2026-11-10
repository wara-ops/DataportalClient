import gc
import gzip
import inspect
import json
import logging
import math
import os
import pickle
import re
import tarfile
import urllib.parse
import weakref
from copy import deepcopy
from pathlib import Path

import pandas as pd
import pytest
import requests
import zstandard
from requests.exceptions import ChunkedEncodingError, HTTPError

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
