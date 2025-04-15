import json
import os
import pandas as pd
import requests
import time

import logging

from pydantic import BaseModel, ValidationError
from datetime import datetime
from dataportal.chunkedlist import ChunkedList
from dataportal.logger import get_logger

_logger = get_logger()

# TODO:
#   Update listFiles to include general index, orderBy and sortBy
#   Update tests (& CI?), replace mock with remote test dataportal?
#   Refactor & update descriptions
#   Check what notebooks need updating?
#   Have CI push dist to open github, have notebooks pull it from there

class Dataset(BaseModel):
    DatasetID: int
    DatasetName: str
    Organization: str
    Category: str
    CreateDate: datetime
    ShortInfo: str
    LongInfo: str
    Tags: list[str]

class File(BaseModel):
    FileID: int | str
    MFileName: str
    OriginName: str
    StartDate: datetime
    StopDate: datetime
    FileSize: int
    MetricEntries: int
    MetricType: str
    ExtraFile: bool

class State(BaseModel):
    dataset: Dataset | None = None
    file: File | None = None

class DataportalClient:
    """
        A class for streamlining access to the WARA-Ops data.

        Methods
        -------
        currentDataset
            return object describing the currently selected dataset, can be converted to json with .model_dump_json(indent=4).
        currentLoadedFile
            return object describing the currently loaded file, can be converted to json with .model_dump_json(indent=4).
        fromDataset
            selects a dataset based on dataset id or name.
        getData
            retrieves data from a file in the selected dataset as a pandas DataFrame.
        getExtraFiles
            downloads the extra files from the selected dataset.
        listFiles
            lists the available files in the selected dataset, returned an iterator object of type ChunkedList.
    """

    token: str
    debug: bool = False
    url: str
    datapath: str
    reconnectAttemps: int = 3
    reconnectWaitTime: int = 5
    relcachepath: str = ".cache/portalclient"
    statefile: str = "state.json"

    def __init__(self, token: str | None = None, debug: bool = False, cleanStart: bool = False):
        """
            Creates a new DataportalClient object with the supplied token. If there exists a previously loaded
            file, the corresponding dataset will be selected by default if possible.

            Parameters
            ----------
            token : str
                the user token used for authenticating with the dataportal.
            debug : bool, optional
                whether to log debug messages. Defaults to False.
            cleanStart : bool, optional
                whether to run a clean client and remove any existing state from previous clients.

            Environment
            ----------
            DATAPORTAL_CLIENT_LOG_LEVEL : str
                can be set to VERBOSE, DEBUG, INFO, WARNING, ERROR.
            DATAPORTAL_API : str
                the url to dataportal API.
            DATAPORTAL_CLIENT_BASEPATH : str
                the local basepath to use for downloading/caching files.
            DATAPORTAL_CLIENT_TOKEN : str
                the user token used for authenticating with the dataportal.

            Returns
            -------
                a new DataportalClient object.
        """

        self._s = requests.Session()

        self.token = token or os.environ.get("DATAPORTAL_CLIENT_TOKEN", None)

        if self.token in [None, '']:
            raise Exception("Need to provide argument token or DATAPORTAL_CLIENT_TOKEN environment variable")

        # handle logging
        if debug:
            _logger.setLevel(logging.DEBUG)
        else:
            try:
                _logger.setLevel(getattr(logging, os.environ.get("DATAPORTAL_CLIENT_LOG_LEVEL", "INFO")))
            except AttributeError as e:
                _logger.warning("Error %s setting log level %s", e, os.environ.get("DATAPORTAL_CLIENT_LOG_LEVEL", "INFO"))

        _logger.info("loglevel %s", logging.getLevelName(_logger.getEffectiveLevel()))

        self.url = os.environ["DATAPORTAL_API"]

        basepath = os.environ.get("DATAPORTAL_CLIENT_BASEPATH", os.environ["HOME"])
        self.cachepath = os.path.join(basepath, self.relcachepath)
        self.statepath = os.path.join(self.cachepath, self.statefile)

        self._checkToken()

        self._stateLoad()
        if cleanStart:
            self._stateReset()

    # Download and cache a new data file into notebook server. Will occupy space from user volume in a hidden directory and thus be persistent.
    # Only 1 file at a time can be cached. Throws an error if a user tries to cache an extrafile.
    def _cacheDataFile(self, fileID: int):
        try:
            fileinfo = self._sendRequest(f"v1/dataset/{self.state.dataset.DatasetID}/files/{fileID}").json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                raise ValueError(self._errTextNoFile(self.state.dataset, fileID))
            else:
                raise e

        if fileinfo['ExtraFile']:
            raise ValueError(f'File with ID {fileID} is an extrafile, not a datafile. Use client.getExtraFiles({fileID}) to obtain it.')

        if self.state.file is not None:
            try:
                os.remove(os.path.join(self.cachepath, self.state.file.MFileName))
            except FileNotFoundError:
                _logger.debug('cached file could not be found')

        self.state.file = File(**fileinfo)

        try:
            self._downloadFile(fileID, self.cachepath)
        except Exception as e:
            self.state.file = None
            raise e
        finally:
            self._stateSave()

    # Validate token towards API
    def _checkToken(self):
        _logger.debug(f"connecting to {self.url}...")
        self._sendRequest("v1/validatetoken")
        _logger.info("Connection OK")

    # Download file on fileID, will retry "self.recconectAttempt" times.
    def _downloadFile(self, fileID, destPath, extrafile=False):
        err: list[Exception] = []
        for attempt in range(self.reconnectAttemps):
            try:
                with self._sendRequest(f"v1/dataset/{self.state.dataset.DatasetID}/files/{fileID}/content", query={'extrafile': 'true' if extrafile else 'false'}, stream=True) as r:
                    filename = r.headers["Content-disposition"].split("filename=")[-1]
                    with open(os.path.join(destPath, filename), "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                break
            except requests.exceptions.ChunkedEncodingError as e:
                err.append(e)
                if attempt + 1 < self.reconnectAttemps:
                    _logger.info(f"Connection failed ({attempt+1}/{self.reconnectAttemps}), retrying...")
                time.sleep(self.reconnectWaitTime)
            except Exception as e:
                raise e
        else:
            _logger.warning(f"Connection failed ({attempt+1}/{self.reconnectAttemps}), throwing error")
            raise Exception(err)

    def _errTextNoFile(self, dataset: Dataset, fileid: int):
        return f'File with ID {fileid} could not be found in dataset {dataset.DatasetName}'

    def _isDatasetLoaded(self):
        if self.state.dataset is None:
            raise NoDatasetError("No loaded dataset could be found, have you selected a dataset?")

    def _isFileLoaded(self):
        if self.state.file is None:
            raise NoDatasetError("No loaded file could be found, have you selected a file?")

    # Creates a pandas dataframe generator from the selected datafile.
    def _partiallyParseData(self, datafile: str, entries: int, **extraArgs):
        def parsePickle():
            yield pd.read_pickle(datafile, **extraArgs)

        if ".pkl" in self.state.file.MFileName:
            return parsePickle()
        elif ".csv" in self.state.file.MFileName:
            return pd.read_csv(datafile, chunksize=entries, **extraArgs)
        elif ".json" in self.state.file.MFileName:
            return pd.read_json(datafile, chunksize=entries, lines=True, **extraArgs)
        else:
            try:  # if can be parsed as json, do so
                return pd.read_json(datafile, chunksize=entries, lines=True, **extraArgs)
            except Exception:
                raise UnsupportedExtentionError(f"file {self.state.file.MFileName} can not be parsed")

    # Performs a requests to the API with the given path and query parameters
    def _sendRequest(self, path: str, query={}, stream=False):
        if query:
            query_string = "?" + "&".join([f"{key}={value}" for (key, value) in query.items()])
        else:
            query_string = ""
        _logger.debug(f"sending {path + query_string}")
        response = self._s.get(os.path.join(self.url, path) + query_string, headers={"Authorization": "Bearer " + self.token}, stream=stream)
        response.raise_for_status()
        return response

     # Load state file if it exists, otherwise create the path if missing
    def _stateLoad(self):
        if os.path.isfile(self.statepath):
            with open(self.statepath, 'r') as f:
                try:
                    self.state = State(**json.load(f))
                except ValidationError as e:
                    _logger.warning(f"WARNING - {e}")
                    self.state = State()
                except Exception as e:
                    _logger.exception(e)
                    self.state = State()
        else:
            os.makedirs(self.cachepath, exist_ok=True)
            self.state = State()

    # Removes loaded file and resets the state
    def _stateReset(self):
        if self.state.file is not None:
            filepath = os.path.join(self.cachepath, self.state.file.MFileName)
            if os.path.isfile(filepath):
                os.remove(filepath)
        if os.path.isfile(self.statepath):
            os.remove(self.statepath)
        self.state = State()

    # Saves the state to file
    def _stateSave(self):
        with open(self.statepath, 'w') as f:
            f.write(self.state.model_dump_json())

    def currentDataset(self):
        """
            Returns the currently loaded dataset. To print it, use print(ds.model_dump_json(indent=4)).
        """
        self._isDatasetLoaded()
        _logger.debug(self.state.dataset.model_dump_json(indent=4))
        return self.state.dataset # JR: model dump json?

    def currentLoadedFile(self):
        """
            Returns the currently loaded file. To print it, use print(ds.model_dump_json(indent=4)).
        """
        self._isDatasetLoaded()
        self._isFileLoaded()
        _logger.debug(self.state.file.model_dump_json(indent=4))
        return self.state.file # JR: model dump json?

    # Selects a dataset to target. Returns reference to current object for chaining
    # Mutates client state
    def fromDataset(self, idOrName: str | int):
        """
            Selects a dataset to consider in future calls. Returns the DataportalClient object to allow chaining, e.g.,

                client.fromDataset('foo').listFiles()

            is the same as calling

                client.fromDataset('foo')
                client.listFiles()

            Use the class method 'currentDataset' to get the currently considered dataset.

            Parameters
            ----------
            idOrName: string | int
                either the id as an integer, or the name as a string, of the dataset to consider.

            Returns
            -------
            the DataportalClient object.

        """

        def getDatasetBy(key):
            datasets = self._sendRequest("v1/dataset").json()["Datasets"]
            for ds in datasets:
                if ds[key] == idOrName:
                    return ds
            raise ValueError(f"No dataset with {key} = {idOrName} found")

        newdataset = None

        if isinstance(idOrName, str) and (self.state.dataset is None or not self.state.dataset.DatasetName == idOrName):
            newdataset = Dataset(**getDatasetBy("DatasetName"))
            _logger.debug("setting new dataset with name")

        elif isinstance(idOrName, int) and (self.state.dataset is None or not self.state.dataset.DatasetID == idOrName):
            newdataset = Dataset(**getDatasetBy("DatasetID"))
            _logger.debug("setting new dataset with id")

        if newdataset is not None:
            self._stateReset()
            self.state = State(dataset=newdataset)
            _logger.debug(f"dataset selected {self.state.dataset.model_dump_json(indent=4)}")

        return self

    # Returns a dataframe (or generator for dataframes) of a certain size for the given fileID.
    # First time, file is loaded into the notebook server, this might take a couple of minutes
    # depending on the size of the file.
    # Calling this function again with the same, or without a, fileID will use the currently loaded file, much quicker
    # Use the class method currentLoadedFile() to see what file is currently loaded.
    # Mutating, non-pure function
    def getData(self, fileID: int | str ="", entries=100000, generator=False, datatype="", **extraArgs):
        """
            Returns a Pandas DataFrame, or a generator of dataframes, of the data in the selected file.
            The first time this function is called with a new fileID, the file is loaded into the notebook
            server. This might take a while depending on the size of the file, up to a couple of minutes if
            the file is large. Calling this function again with the same, or without a, fileID will use the
            currently loaded file which will be much quicker. Changing the selected dataset will always
            reload the file. Use the class method 'currentLoadedFile' to see which file is currently loaded.

            How files are loaded via pandas:
            .json (or empty filename)
                pandas.read_json(file, chunksize=entries, lines=True, **extraArgs)
            .pkl
                pandas.read_pickle(file, **extraArgs)
            .csv
                pandas.read_csv(file, chunksize=entries, **extraArgs)

            Parameters
            ----------
            fileID : int | string, optional
                the fileID of the file to be used. Has to be int or an int parsable string. If left empty
                the program will try to use the currently loaded file if it exits, if it does not an
                error will be raised.
            entries : int, optional
                the number of entries to be included in the dataframe. Only valid for chunkable file formats,
                e.g. not .pkl files. If set to -1, all entries will be read. Use this at your own risk for
                larger files. Defaults to 100000.
            generator : bool, optional
                if a generator of dataframes over the data should be returned. This allows for iterating over
                all data in the file without loading it all into memory. Each subsequent dataframe yields the
                next "entries" number of data points. If false, the returned dataframe will include the first
                "entries" number of data points. Defaults to False.
            extraArgs : optional key-value arguments as (if) required
                extra arguments to pass to pandas when loading the file.

            Returns
            -------
            a Pandas DataFrame if generator=False, else a generator yielding dataframes.
        """

        self._isDatasetLoaded()

        if fileID == "":
            if self.state.file is None:
                raise ValueError("Either fileID has to be set, or there must be a previously loaded file")
            fileID = self.state.file.FileID

        fileID = int(fileID)

        if self.state.file is None or self.state.file.FileID != fileID:
            self._cacheDataFile(fileID)

        datafile = os.path.join(self.cachepath, self.state.file.MFileName)

        if datatype:
            _logger.warning('WARNING - deprecated: argument "datatype" is no longer in use')

        df_generator = self._partiallyParseData(datafile, entries, **extraArgs)

        # Generator can be returned, but the default is the first dataframe
        if generator:
            return df_generator
        else:
            return next(df_generator)


    # give fileID as int or intparsable string, can also give list of fileIDs. no fileID supplied will fetch all
    def getExtraFiles(self, fileID: int | str | list[int | str] = '') -> tuple[list[int], dict[int, Exception]]:
        """
            Downloads the selected extrafiles for the currently loaded dataset into the current directory.

            Parameters
            ----------
            fileID : int | string | list[int | str], optional
                the fileID(s) of the extrafile(s) to be downloaded. Has to be int or an int parsable string,
                or a list of integers or int parsable strings. The target file must be a designated extrafile,
                or an error will be thrown.

            Returns
            -------
            returns a tuple with (id_list, failures), where
                id_list: list[int]
                failures: dict with ids as keys and exceptions as value
        """

        self._isDatasetLoaded()
        fileID_intlist = []

        if isinstance(fileID, int | str):
            if fileID == '':
                pass
            else:
                fileID_intlist = [int(fileID)]
        elif isinstance(fileID, list):
            fileID_intlist = fileID
            for (i, id) in enumerate(fileID):
                fileID_intlist[i] = int(id)
        elif fileID is None:
            pass
        else:
            raise ValueError(f'Wrong input type of fileID {type(fileID)}')
        ids = []
        failures = {}
        for id_ in fileID_intlist:
            try:
                self._downloadFile(id_, "", extrafile=True)
                ids.append(id_)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    _logger.warning("Download failed for extra file with FileID %s", id_)
                    failures[id_] = ValueError(self._errTextNoFile(self.state.dataset, id_))
                else:
                    _logger.exception("Download failed for extra file with FileID %s", id_)
                    failures[id_] = e
            except Exception as e:
                _logger.eception("Download failed for extra file with FileID %s", id_)
                failures[id_] = e

        return ids, failures

    # JR: It would be good if we could give some deprecation hints/warning on the old list function. E.g. that we allow to call this function with prettyPrint and returnList but
    # throw an deprecation error (or warning?) with a hint on how the new list function should be used. Otherwise peoples notebooks will start to fail seemingly at random
    # JR: Also, demo notebooks needs to be updated to use this new listFiles
    def listFiles(self, index=None, limit=500, orderBy='FileID', sort='asc', startDate=None, endDate=None, filter=None, excludeFilter=None, extraFiles=False):
        """
            Lists the available files in the selected dataset.

            Parameters
            ----------
            index : int, optional
                index to start returning items from. Defaults to the index of the first item in the list.
            limit : int, optional
                number of items in the ChunkedList buffer. Defaults to 500.
            orderBy: string, optional
                what field index should use and the list be ordered by. Defaults to FileID.
            sort: string', optional
                the sort direction, can be either 'asc' or 'desc'. Defaults to 'asc'.
            startDate : string, optional
                return files with dates newer then this date. Defaults to None.
            endDate : string, optional
                return files with dates older then this date. Defaults to None.
            filter : string, optional
                filtering string for returned files, follows SQL LIKE syntax. Defaults to None.
            excludeFilter : string, optional
                exclude filtering string for returned files, follows SQL LIKE syntax. Defaults to None.
            extraFiles : bool, optional
                includes extrafiles in the list if set to true. Default False.

            Returns
            -------
            iterator object of type ChunkedList.
        """

        self._isDatasetLoaded()

        # JR: add __str__ or similar to ChunkedList, so that running it in jupyter without saving the variable prints the available files
        return ChunkedList(
            url=self.url,
            path=f"v1/dataset/{self.state.dataset.DatasetID}/files",
            token=self.token,
            index=index,
            limit=limit,
            sort=sort,
            orderBy=orderBy,
            startDate=startDate,
            endDate=endDate,
            filter=filter,
            excludeFilter=excludeFilter,
            extraFiles=extraFiles
        )

class UnsupportedExtentionError(Exception):
    pass


class NoDatasetError(Exception):
    pass


class DeprecationError(Exception):
    pass
