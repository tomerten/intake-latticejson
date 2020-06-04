from intake.source import base
from urllib.request import urlopen
from json import loads

class Latticejson(base.DataSource):
    containter = 'python'
    version    = '0.0.1'
    partition_access = False
    name = 'lattice_json'


    def __init__(self, url, metadata=None):
        self.url = url
        super(Latticejson, self).__init__(metadata=metadata)

    def _get_schema(self):
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
        res = urlopen(self.url).read()
        out = loads(res)
        self.metadata = {
                'version': out.get('version'),
                'title': out.get('title'),
                'root': out.get('root')
                }
        return loads(res)
