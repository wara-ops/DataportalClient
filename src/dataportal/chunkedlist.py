import os
import functools
import requests
import logging

from frozendict import frozendict
from typing import Optional
from urllib.parse import quote

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
        Requests to the remote server are cached to speed up listing and subsequent calls. At most 8192
        calls are cached per default. In special cases when request cache needs to be cleared due to
        server updates, call this to clear cache: ChunkedList._sendRequest.cache_clear().
    """

    def __init__(self,
                 url: str,
                 path: str,
                 token: str,
                 index: Optional[int | str] = None,
                 limit: int = 2000,
                 sort: str = 'asc',
                 orderBy: Optional[str] = None,
                 startDate: Optional[str] = None,
                 endDate: Optional[str] = None,
                 filter: Optional[str] = None,
                 excludeFilter: Optional[str] = None,
                 extraFiles: bool = False,
                 allowedOrderFields: list = [
                    "FileID",
                    "MFileName",
                    "StartDate",
                    "StopDate",
                    "FileSize",
                    "MetricEntries"
                ]
            ):

        # Some assertions at creation
        if orderBy not in allowedOrderFields:
            raise ValueError(f"orderBy must be one of {allowedOrderFields}, not '{orderBy}'")
        elif sort not in ['asc', 'desc']:
            raise ValueError(f"sort must be one of ['asc', 'desc'], not '{sort}'")

        self._s = requests.Session()

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
    @freezeargs
    @functools.lru_cache(maxsize=8*1024)
    def _sendRequest(self, path: str, query: Optional[dict] = None, stream: bool = False):
        """
            Sends the requests to the remote server. This function have a lru cache to speed subsequent
            requests to the server (this could lead to new data not beeing fetched). Cache saves at
            most 8196 function calls, and can be reset with .cache_clear().
        """
        if query is None:
            query = {}
        if query:
            queryString = "?" + "&".join([f"{key}={quote(str(value))}" for (key, value) in query.items() if value is not None])
        else:
            queryString = ""
        # _logger.info(f"sending {path + queryString}")
        _logger.info("sending %s%s", os.path.join(self._url, path), queryString)
        response = self._s.get(os.path.join(self._url, path) + queryString, headers={"Authorization": "Bearer " + self._token}, stream=stream)
        response.raise_for_status()
        return response

    def _fetch_data(self, index: Optional[int | str]):
        """
            Fetches data and caches the data locally in the object.
        """

        _logger.debug(f"fetch data index={index} " +
                      f"limit={self._limit} " +
                      f"order_by={self._orderBy} " +
                      f"sort={self._sort} " +
                      f"start_date={self._startDate} " +
                      f"end_date={self._endDate} " +
                      f"filter={self._filter} " +
                      f"excludeFilter={self._excludeFilter} " +
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

        _logger.debug(f"self._index = {self._index}, " +
                      f"self._currentPageIndex = {self._currentPageIndex}, " +
                      f"self._nextPageIndex = {self._nextPageIndex}, " +
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

            Parameters
            ----------
            index : int
                index is the remote id, not the index in the file list.
        """
        try:
            data = self._sendRequest(f"{self._path}/{index}").json()
            return data
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # keyerror next otherwise
                pass
            else:
                raise e
        try:
            data = self._sendRequest(f"{self._path}/{index}", query={"extrafile": 'true'}).json()
            return data
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                raise KeyError("FileID {index} does not exist")

    def __len__(self):
        """
            Override len to make it behave as a list/iterator.
        """
        return self.getCount()