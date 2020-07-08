from intake.source import base
from intake.container.base import RemoteSource, get_partition
from urllib.request import urlopen, Request
from json import loads
import dask
import fsspec
from latticejson.convert import to_elegant, to_madx


import datetime
import pandas as pd
from intake.container.base import RemoteSource, get_partition
from intake.source.base import Schema

def get_twisscolumns(tfsfile):
    """
    Reads the headers of the columns of the twiss file
    :param tfsfile:  file where twiss output is stored
    :return: list of strings
    """
    cols = pd.read_csv(tfsfile, delim_whitespace=True, skiprows=range(46), nrows=2, index_col=None)
    return list(cols.columns[1:].values)

def get_tfsheader(tfsfile):
    """
    Read the header part of the twiss data (e.g. particle energy, mass, etc...)
    :param tfsfile: file where twiss output is stored
    :return: dataframe containing the header data
    """
    headerdata = pd.read_csv(tfsfile, delim_whitespace=True, nrows=44, index_col=None)
    headerdata.columns = ['AT', 'NAME', 'TYPE', 'VALUE']
    return headerdata[['NAME', 'VALUE']]


def get_twissdata(tfsfile):
    """
    Get the actual table data of the twiss data
    :param tfsfile: file where the twiss data is stored
    :return: dataframe containing the twiss table data
    """
    data = pd.read_csv(tfsfile, delim_whitespace=True, skiprows=48, index_col=None, header=None)
    data.columns = get_twisscolumns(tfsfile)
    return data


def get_survey_columns(tfssurveyfile):
    """
    Reads the headers of the columns of the survey file
    :param tfssurveyfile:  file where survey output is stored
    :return: list of strings
    """
    cols = pd.read_csv(tfssurveyfile, delim_whitespace=True, skiprows=range(6), nrows=2, index_col=None)
    return cols.columns[1:].values


def get_survey_data(tfssurveyfile):
    """
    Get the actual table data of the twiss data
    :param tfssurveyfile: file where the twiss data is stored
    :return: dataframe containing the twiss table data
    """
    data = pd.read_csv(tfssurveyfile, delim_whitespace=True, skiprows=8, index_col=None, header=None)
    data.columns = get_survey_columns(tfssurveyfile)
    return data



class Latticejson(base.DataSource):
    containter = 'python'
    version    = '0.0.1'
    partition_access = False
    name = 'lattice_json'


    def __init__(self, filename, metadata=None):
        self.filename = filename
        self.cache = None
        super(Latticejson, self).__init__(metadata=metadata)

    def _load(self):
        with open(self.filename) as f:  
            self.cache = loads(f.read())

    def _get_schema(self):
        if self.cache is None:
            self._load()

        self._dtypes = {
                'version': 'str',
                'title': 'str',
                'root': 'str',
                'elements': 'dict',
                'lattice': 'dict'
                }
        return base.Schema(
                datashape=None,
                dtype=self._dtypes,
                shape=(None, len(self._dtypes)),
                npartitions=1,
                extra_metadata={}
                )

    def _get_partition(self, _):
        if self._dict is None:
            self._load_metadata()
        data = [self.read()]

        return data


    def read(self):
        if self._dict is None:
            self._load()

        self.metadata = {
                'version': self._dict.get('version'),
                'title': self._dict.get('title'),
                'root': self._dict.get('root')
                }

        return self.cache

    def to_madx(self):
        if self.cache is None:
            self._load()
        print(self.cache)
        return self.cache

    def _close(self):
        pass

class LocalLatticejson(RemoteSource):
    name      = 'local-latticejson'
    container = 'python'
    partition_access = False
    
    def __init__(self, filename, parameters= None, metadata=None, **kwargs):
        # super().__init__(org, repo, filename, parameters, metadata=metadata, **kwargs)
        self._schema = None
        self.filename = filename
        self.metadata = metadata
        self._dict = None

    def _load(self):
        self._dict = read_local_file(self.filename)

    def _get_schema(self):
        if self._dict is None:
            self._load()

        self._dtypes = {
                'version': 'str',
                'title': 'str',
                'root': 'str',
                'elements': 'dict',
                'lattice': 'dict'
                }
        return base.Schema(
                datashape=None,
                dtype=self._dtypes,
                shape=(None, len(self._dtypes)),
                npartitions=1,
                extra_metadata={}
                )

    def _get_partition(self, i):
        if self._dict is None:
            self._load_metadata()
        data = [self.read()]
        return [self._dict]


    def read(self):
        print('here')
        if self._dict is None:
            self._load()

        self.metadata = {
                'version': self._dict.get('version'),
                'title': self._dict.get('title'),
                'root': self._dict.get('root')
                }

        return self._dict

    def to_madx(self):
        self._get_schema()
        return to_madx(self._dict)

    def _close(self):
        pass





class RemoteLatticejson(RemoteSource):
    """
    A lattice json source on the server
    """

    name      = 'remote-latticejson'
    container = 'python'
    partition_access = False

    def __init__(self,org, repo, filename, parameters= None, metadata=None, **kwargs):
        # super().__init__(org, repo, filename, parameters, metadata=metadata, **kwargs)
        self._schema = None
        self.org = org
        self.repo = repo
        self.filename = filename
        self.metadata = metadata

        self._dict = None

    def _load(self):
        self._dict = read_remote_file(self.org, self.repo, self.filename)

    def _get_schema(self):
        if self._dict is None:
            self._load()

        self._dtypes = {
                'version': 'str',
                'title': 'str',
                'root': 'str',
                'elements': 'dict',
                'lattice': 'dict'
                }
        return base.Schema(
                datashape=None,
                dtype=self._dtypes,
                shape=(None, len(self._dtypes)),
                npartitions=1,
                extra_metadata={}
                )


    def _get_partition(self, i):
        if self._dict is None:
            self._load_metadata()
        data = [self.read()]
        return [self._dict]


    def read(self):
        if self._dict is None:
            self._load()

        self.metadata = {
                'version': self._dict.get('version'),
                'title': self._dict.get('title'),
                'root': self._dict.get('root')
                }
        
        return self._dict

    def to_madx(self):
        self._get_schema()
        return to_madx(self._dict)

    def _close(self):
        pass


def read_remote_file(org, repo, filename):
    """
    Read the remote JSON file and return as dict.

    Parameters:
    -----------
    org: str
        organization on Github
    repo: str
        repo name on Github
    filename: str
        filename of JSON file to get
    """
    import fsspec
    fs = fsspec.filesystem('github', org=org, repo=repo)

    with fs.open(filename) as f:
        data = loads(f.read())

    return data

def read_local_file(filename):
    """
    Read the remote JSON file and return as dict.

    Parameters:
    -----------
    org: str
        organization on Github
    repo: str
        repo name on Github
    filename: str
        filename of JSON file to get
    """
    import fsspec
    fs = fsspec.filesystem('file')

    with fs.open(filename) as f:
        data = loads(f.read())

    return data

