import os
import pytest
import json
import pandas as pd
import logging
import datetime

import requests

from tests.settings import (
    systests_testuser,
    systests_testpw,
    systests_url,
    systests_datasetname,
    mockdownloaddir,
    check_cache,
    check_extra_files,
    verify_files
)

from requests.exceptions import HTTPError

from src.dataportal import DataportalClient
from src.dataportal.dataportalclient import NoDatasetError, Dataset, ChunkedList

if "DATAPORTAL_API" not in os.environ:
    os.environ["DATAPORTAL_API"] = systests_url
else:
    print(f"DATAPORTAL_API env variable already set to {os.environ['DATAPORTAL_API']}")


s = requests.Session()
def session_post(path, json, headers={}):
    response = s.post(os.path.join(os.environ["DATAPORTAL_API"], path), json=json, headers=headers)
    response.raise_for_status()
    return response

def session_get(path, headers={}):
    response = s.get(os.path.join(os.environ["DATAPORTAL_API"], path), headers=headers)
    response.raise_for_status()
    return response

# test connection
session_get('v1/test')

# login
session_post('v1/login', {'username': systests_testuser, 'password': systests_testpw})

# get token
token = session_get('v1/generatetoken').json()['access_token']


# Load the dataset, files and extrafiles from the test system matching the testdataset name
datasets = session_get('v1/dataset').json()['Datasets']

try:
    testdataset = [ds for ds in datasets if ds['DatasetName'] == systests_datasetname].pop()
except IndexError:
    raise ValueError(f"Cannot find testdataset {systests_datasetname} in test system")

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
        if os.path.isdir(mockdownloaddir):
            dsfilenames = [file['MFileName'] for file in testextrafiles]
            for filename in os.listdir(mockdownloaddir):
                if filename in dsfilenames:
                    os.remove(os.path.join(mockdownloaddir, filename))
        else:
            os.mkdir(mockdownloaddir)

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