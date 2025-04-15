import os
import pytest
import json
import pandas as pd
import re
import math
import logging

import urllib.parse
from copy import deepcopy

from tests.settings import (
    mockurl,
    mockdsmetrics,
    mockdslogs,
    mockdserr,
    mockdatasetids,
    mockdatasets,
    mockdatasetmap,
    metricfilesext,
    logfilesext,
    mockfiles,
    mockfilesdir,
    mockdownloaddir,
    testfiles,
    extrafiles,
    metricfile_missingcomp,
    logfile_missingcomp,
    mockdatashapes,
    mocktoken,
    check_cache,
    strtobool,
    check_extra_files
)

from requests.exceptions import HTTPError, ChunkedEncodingError

from src.dataportal import DataportalClient
from src.dataportal.dataportalclient import NoDatasetError, UnsupportedExtentionError, Dataset, File, ChunkedList

os.environ["DATAPORTAL_API"] = mockurl

@pytest.fixture(scope="session", autouse=True)
def generate_testfiles():

    def readDF(filepath):
        if '.csv' in filepath:
            return pd.read_csv(filepath)
        elif '.json' in filepath:
            return pd.read_json(filepath, lines=True)
        else:
            raise UnsupportedExtentionError(filepath)

    def writeDF(df, filename, extraArgs={}):
        filepath = os.path.join(mockfilesdir, filename)
        if '.pkl' in filename:
            df.to_pickle(filepath, **extraArgs)
        elif '.csv' in filename:
            df.to_csv(filepath, **{'index': False, **extraArgs})
        elif '.json' in filename:
            df.to_json(filepath, **{'orient': 'records', 'lines': True, **extraArgs})
        else:
            df.to_json(filepath, **{'orient': 'records', 'lines': True, **extraArgs})

    def genFilesFromDF(filepath, file_extensions, nocomp={}):
        df = readDF(filepath)
        col0 = df.columns[0]
        filename = os.path.basename(filepath).split('.')[0]
        for fext in file_extensions:
            newfilename = filename + fext
            newdf = df.rename(columns={col0: newfilename})
            writeDF(newdf, newfilename)

        if nocomp:
            newdf_nocomp = df.rename(columns={col0: nocomp['name']})
            writeDF(newdf_nocomp, nocomp['name'], extraArgs={'compression': nocomp['compression']})

    def createEmptyFiles(filenames: list[str]):
        for filename in filenames:
            with open(os.path.join(mockfilesdir, filename), 'w') as fp:
                fp.write(f'This is extrafile {filename}')
                pass

    if not os.path.isdir(mockfilesdir):
        os.mkdir(mockfilesdir)

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
        self.file = open(self.filename, 'rb')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()
        return None

@pytest.fixture(autouse=True)
def mocked_responses(mocker):
    mockresp = MockedResponse()

    def mockfunc(url, headers={}, stream=False):

        urlParts = urllib.parse.urlparse(url)
        if headers["Authorization"].split(' ')[-1] != mocktoken:
            mockresp.status_code = 400
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
                raise HTTPError(response=mockresp)
            except Exception as e:
                print("error: get content, ", e)
                mockresp.status_code = 500
                raise HTTPError(response=mockresp)

            if (datasetid == mockdatasetids[mockdserr]):
                if file['OriginName'] == 'HTTPError':
                    raise HTTPError
                elif file['OriginName'] == 'ChunkedEncodingError1' and mockresp.calls[(datasetid, fileid)] == 1:
                    raise ChunkedEncodingError
                elif file['OriginName'] == 'ChunkedEncodingErrorX':
                    raise ChunkedEncodingError
                elif file['OriginName'] == 'Exception':
                    raise Exception('mocked exception')

            mockresp.headers = {"Content-disposition": "filename=" + filename}
            mockresp.filename = os.path.join(mockfilesdir, filename)

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
        if os.path.isdir(mockdownloaddir):
            dsfilenames = [file['MFileName'] for file in mockfiles[mockdatasetids[mockdserr]]]
            for filename in os.listdir(mockdownloaddir):
                if filename in dsfilenames:
                    os.remove(os.path.join(mockdownloaddir, filename))
        else:
            os.mkdir(mockdownloaddir)

    def test_download_file(self, client: DataportalClient):
        file = next(filter(lambda o: o['OriginName'] == 'thisworks', client.listFiles()))

        client._downloadFile(file['FileID'], mockdownloaddir)

        assert len(os.listdir(mockdownloaddir)) > 0
        assert file['MFileName'] in os.listdir(mockdownloaddir)
        assert os.path.getsize(os.path.join(mockdownloaddir, file['MFileName'])) > 0


    def test_download_file_retries(self, client: DataportalClient, caplog):

        with caplog.at_level(logging.INFO):
            file = next(filter(lambda o: o['OriginName'] == 'ChunkedEncodingError1', client.listFiles()))

            client._downloadFile(file['FileID'], mockdownloaddir)

            # assert that 1 error has been thrown
            strmatch = f'Connection failed (1/{client.reconnectAttemps}), retrying...'
            assert strmatch in caplog.text

            assert len(os.listdir(mockdownloaddir)) > 0
            assert file['MFileName'] in os.listdir(mockdownloaddir)
            assert os.path.getsize(os.path.join(mockdownloaddir, file['MFileName'])) > 0

    def test_download_file_too_many_retries(self, client: DataportalClient, caplog):
        with caplog.at_level(logging.INFO):
            file = next(filter(lambda o: o['OriginName'] == 'ChunkedEncodingErrorX', client.listFiles()))

            with pytest.raises(Exception):
                client._downloadFile(file['FileID'], mockdownloaddir)

            # assert that all recconection attempts have been made
            strmatch = f"Connection failed ({client.reconnectAttemps}/{client.reconnectAttemps}), throwing error"
            assert strmatch in caplog.text

            # file has not been downloaded
            assert file['MFileName'] not in os.listdir(mockdownloaddir)

    def test_download_file_general_exception_handling(self, client: DataportalClient):
        file = next(filter(lambda o: o['OriginName'] == 'Exception', client.listFiles()))

        with pytest.raises(Exception) as e:
            client._downloadFile(file['FileID'], mockdownloaddir)

        # the correct error message has been returned in exception
        assert str(e.value) == 'mocked exception'

        # file has not been downloaded
        assert file['MFileName'] not in os.listdir(mockdownloaddir)


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
        assert len(os.listdir(client_preloaded.cachepath)) == 2

        client_clean = DataportalClient(mocktoken, cleanStart=True)
        assert client_clean.state.dataset is None
        assert client_clean.state.file is None
        assert len(os.listdir(client_preloaded.cachepath)) == 0

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
        assert len(os.listdir(client_clean.cachepath)) == 0
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
        if os.path.isdir(mockdownloaddir):
            dsfilenames = [file['MFileName'] for file in mockfiles[mockdatasetids[mockdsmetrics]]]
            for filename in os.listdir(mockdownloaddir):
                if filename in dsfilenames:
                    os.remove(os.path.join(mockdownloaddir, filename))
        else:
            os.mkdir(mockdownloaddir)

    @pytest.fixture(autouse=True)
    def returnToTestDir(self):
        testdir = os.getcwd()
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
        except StopIteration:
            raise ValueError('need atleast 2 extrafiles in selected dataset for this test')

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
        assert len(os.listdir(mockdownloaddir)) == 1

    def test_download_extrafile_raise_error_on_datafile(self, client: DataportalClient):
        os.chdir(mockdownloaddir)
        datafile = next(filter(lambda file: not file['ExtraFile'], client.listFiles(extraFiles=True)))

        with pytest.raises(ValueError):
            ids, failures = client.getExtraFiles(datafile['FileID'])
            raise failures[datafile['FileID']]

        # No file downloaded
        assert len(os.listdir(mockdownloaddir)) == 0

    def test_download_extrafile_bad_string(self, client: DataportalClient):
        os.chdir(mockdownloaddir)

        with pytest.raises(ValueError):
            ids, failures = client.getExtraFiles("hej")

        # No file downloaded
        assert len(os.listdir(mockdownloaddir)) == 0

        with pytest.raises(ValueError):
            ids, failures = client.getExtraFiles(["hej"])

        # No file downloaded
        assert len(os.listdir(mockdownloaddir)) == 0
