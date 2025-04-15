import os
import itertools
import json
import re

import pandas as pd

from src.dataportal import DataportalClient
from src.dataportal.dataportalclient import State, File

testsbasepath =  os.path.dirname(__file__)

os.environ["DATAPORTAL_CLIENT_BASEPATH"] = testsbasepath

### For the unit tests ###

mockurl =  "http://mockurl"

mockdsmetrics = 'mockmetrics'
mockdslogs = 'mocklogs'
mockdserr = 'mockerr'

mockdatasetids = {}
mockdatasetids[mockdsmetrics] = 0
mockdatasetids[mockdslogs] = 1
mockdatasetids[mockdserr] = 2

mockdatasets = {
    'Datasets': [
        {
            'DatasetID': mockdatasetids[mockdsmetrics],
            'DatasetName': mockdsmetrics,
            'Organization': 'mockorg',
            'Category': 'metric',
            'CreateDate': '2023-01-01T01:02:03Z',
            'ShortInfo': 'This is a mock metric dataset',
            'LongInfo': 'This is a mock metric datasets longer info',
            'Tags': ['mock', 'metrics']
        },
        {
            'DatasetID': mockdatasetids[mockdslogs],
            'DatasetName': mockdslogs,
            'Organization': 'mockorg',
            'Category': 'log',
            'CreateDate': '2023-02-02T01:02:03Z',
            'ShortInfo': 'This is a mock log dataset',
            'LongInfo': 'This is a mock log datasets longer info',
            'Tags': ['mock', 'logs']
        },
        {
            'DatasetID': mockdatasetids[mockdserr],
            'DatasetName': mockdserr,
            'Organization': 'mockorg',
            'Category': 'metric',
            'CreateDate': '2023-02-02T01:02:03Z',
            'ShortInfo': 'This dataset will throw error if files are read from it',
            'LongInfo': 'This dataset will throw error if files are read from it',
            'Tags': ['mock', 'error']
        },
    ]
}

mockdatasetmap = {
    mockdsmetrics: next(filter(lambda ds: ds['DatasetID'] == mockdatasetids[mockdsmetrics], mockdatasets['Datasets'])),
    mockdslogs: next(filter(lambda ds: ds['DatasetID'] == mockdatasetids[mockdslogs], mockdatasets['Datasets'])),
    mockdserr: next(filter(lambda ds: ds['DatasetID'] == mockdatasetids[mockdserr], mockdatasets['Datasets']))
}

testfiles = {
    'metric': os.path.join(testsbasepath, 'testfiles/metricfile.csv'),
    'log': os.path.join(testsbasepath, 'testfiles/logfile.json')
}

extrafiles = {
    'metric': ['extrafile1.md', 'extrafile2.txt'],
    'log': []
}

errdstypes = [
    'thisworks',
    'HTTPError',
    'ChunkedEncodingError1',
    'ChunkedEncodingErrorX',
    'Exception'
]

mockdatashapes = {
    'metric': pd.read_csv(testfiles['metric']).shape,
    'log': pd.read_json(testfiles['log'], lines=True).shape
}

mockfilesdir = os.path.join(testsbasepath, 'generated_testfiles')
mockdownloaddir = os.path.join(testsbasepath, 'download_testfiles')
mocktoken = 'asd123'

compressions = ['gz', 'bz2', 'zip', 'xz', 'zst', 'tar', 'tar.gz', 'tar.xz', 'tar.bz2']
metricfilesext = ['.pkl', '.csv']
logfilesext = ['', '.json']


metricfile_missingcomp = {
    'name': 'metricfile_missingcomp.pkl',
    'compression': 'bz2'
}

logfile_missingcomp = {
    'name': 'logfile_missingcomp',
    'compression': 'bz2'
}

metricfilesext += ['.'.join(t) for t in itertools.product(metricfilesext, compressions)]
logfilesext += ['.'.join(t) for t in itertools.product(logfilesext, compressions)]

mockfiles = {}
mockfiles[mockdatasetids[mockdsmetrics]] = []
for efile in extrafiles['metric']:
    mockfiles[mockdatasetids[mockdsmetrics]].append(
        {
                "FileID": len(mockfiles[mockdatasetids[mockdsmetrics]]),
                "MFileName": efile,
                "OriginName": 'extrafile',
                "StartDate": None,
                "StopDate": None,
                "FileSize": 12,
                "MetricEntries": None,
                "MetricType": None,
                "ExtraFile": True
        }
    )
for i, fext in enumerate(metricfilesext):
    mockfiles[mockdatasetids[mockdsmetrics]].append(
        {
            "FileID": len(mockfiles[mockdatasetids[mockdsmetrics]]),
            "MFileName": 'metricfile' + fext,
            "OriginName": 'somedatasource',
            "StartDate": f'2023-02-{i+1:02}T00:00:00Z',
            "StopDate": f'2023-02-{i+2:02}T00:00:00Z',
            "FileSize": 1000000,
            "MetricEntries": 1000000,
            "MetricType": 'float',
            "ExtraFile": False
        },
    )
mockfiles[mockdatasetids[mockdsmetrics]].append(
    {
        "FileID": len(mockfiles[mockdatasetids[mockdsmetrics]]),
        "MFileName": metricfile_missingcomp['name'],
        "OriginName": 'somedatasource',
        "StartDate": '2023-03-20T00:00:00Z',
        "StopDate": '2023-03-21T00:00:00Z',
        "FileSize": 1000000,
        "MetricEntries": 1000000,
        "MetricType": 'float',
        "ExtraFile": False
    }
)

mockfiles[mockdatasetids[mockdslogs]] = []
for efile in extrafiles['log']:
    mockfiles[mockdatasetids[mockdslogs]].append(
        {
                "FileID": len(mockfiles[mockdatasetids[mockdsmetrics]]) + len(mockfiles[mockdatasetids[mockdslogs]]),
                "MFileName": efile,
                "OriginName": 'extrafile',
                "StartDate": None,
                "StopDate": None,
                "FileSize": 12,
                "MetricEntries": None,
                "MetricType": None,
                "ExtraFile": True
        }
    )
for fext in logfilesext:
    mockfiles[mockdatasetids[mockdslogs]].append(
    {
        "FileID": len(mockfiles[mockdatasetids[mockdsmetrics]]) + len(mockfiles[mockdatasetids[mockdslogs]]),
        "MFileName": 'logfile' + fext,
        "OriginName": 'somedatasource',
        "StartDate": '2023-04-01T00:00:00Z',
        "StopDate": '2023-04-02T00:00:00Z',
        "FileSize": 1000000,
        "MetricEntries": 1000000,
        "MetricType": 'str',
        "ExtraFile": False
    }
)
mockfiles[mockdatasetids[mockdslogs]].append(
    {
        "FileID": len(mockfiles[mockdatasetids[mockdsmetrics]]) + len(mockfiles[mockdatasetids[mockdslogs]]),
        "MFileName": logfile_missingcomp['name'],
        "OriginName": 'somedatasource',
        "StartDate": '2023-05-01T00:00:00Z',
        "StopDate": '2023-05-02T00:00:00Z',
        "FileSize": 1000000,
        "MetricEntries": 1000000,
        "MetricType": 'str',
        "ExtraFile": False
    }
)

mockfiles[mockdatasetids[mockdserr]] = []
for (i, errtype) in enumerate(errdstypes):
    mockfiles[mockdatasetids[mockdserr]].append(
        {
            "FileID": len(mockfiles[mockdatasetids[mockdsmetrics]]) \
                    + len(mockfiles[mockdatasetids[mockdslogs]]) \
                    + i,
            "MFileName": 'metricfile' + metricfilesext[i],
            "OriginName": errtype,
            "StartDate": f'2023-06-{i+1:02}T00:00:00Z',
            "StopDate": f'2023-06-{i+2:02}T00:00:00Z',
            "FileSize": 1000000,
            "MetricEntries": 1000000,
            "MetricType": 'str',
            "ExtraFile": False
        }
    )

assert (lambda ids: len(set(ids)) == len(ids))(
    [file['FileID'] for filelist in mockfiles.values() for file in filelist]
), "Mock file IDs are not unique!"
assert all([ "ExtraFile" in f for mf in mockfiles.values() for f in mf])

### For the system tests ###

systests_testuser = "user01"
systests_testpw = "password01"

systests_url = "http://localhost:3001"

systests_datasetname = 'metricdumps'

### Common test functions ###

def check_cache(client: DataportalClient, dataFileCached=True):
    print(client.cachepath)
    files = os.listdir(client.cachepath)

    # Only one loaded file + statefile at any time
    assert len(files) == 1 + dataFileCached

    # Check that statefile exists and has size
    localStateFile = client.statepath
    assert os.path.isfile(localStateFile)
    assert os.path.getsize(localStateFile) > 0

    if dataFileCached:
        # Check that cached file exist and has size
        localFile = os.path.join(client.cachepath, client.state.file.MFileName)
        assert os.path.isfile(localFile)
        assert os.path.getsize(localFile) > 0

    # Check that the client state and the saved state are the same
    with open(localStateFile, 'r') as f:
        stateFromFile = State(**json.load(f))
    assert stateFromFile.file == client.state.file
    assert stateFromFile.dataset == client.state.dataset

def check_extra_files(extrafiles: list[File]):
    assert len(os.listdir(mockdownloaddir)) == len(extrafiles)
    for file in extrafiles:
        assert file['ExtraFile']
        assert file['MFileName'] in os.listdir(mockdownloaddir)
        assert os.path.getsize(os.path.join(mockdownloaddir, file['MFileName'])) > 0

def verify_files(files, key, regexs, func="match"):
    match = []
    no_match = []
    print(f"################ len(files) {len(files)}")
    for file in files:
        # print(f"file {file}")
        for regex in regexs:
            res = getattr(re, func)(regex, str(file[key]))
            # fix this
            if res:
                match.append((res, file))
            else:
                no_match.append((regex, file))
    return match, no_match

def strtobool (val):
    """Convert a string representation of truth to true (1) or false (0).
    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.
    """
    val = val.lower()
    if val in ('y', 'yes', 't', 'true', 'on', '1'):
        return 1
    elif val in ('n', 'no', 'f', 'false', 'off', '0'):
        return 0
    else:
        raise ValueError("invalid truth value %r" % (val,))