import requests
import json
import os.path


class MBZAPI(object):
    """
    Base class for all MusicBrainz API contact
    """
    BASE_URL = 'https://musicbrainz.org/ws/2/'
    APP_HEADERS = {'User-Agent': 'NYPhil Concert Builder/0.01 \
                    (https://github.com/drewmcdonald/nyphil-program-generator)'}

    def __init__(self, endpoint: str, mbz_id: str = None):
        self.endpoint: str = endpoint
        self.mbz_id: str = mbz_id
        self.request_status_code: int = None
        self.content: dict = None

    def __repr__(self):
        return f'<MBZAPI at endpoint {self.endpoint}>'

    @property
    def is_retrieved(self):
        return self.request_status_code == 200

    @property
    def request_url(self):
        return os.path.join(self.BASE_URL, self.endpoint, self.mbz_id)

    @property
    def request_params(self):
        raise NotImplementedError

    def post_retrieve(self):
        raise NotImplementedError

    def retrieve(self) -> int:
        """make an MBZ API Request, then call the class's post-retrieve method

        :return: request's HTTP status code"""
        result = requests.get(self.request_url, params=self.request_params, headers=self.APP_HEADERS)

        status = result.status_code
        self.request_status_code = status

        if status == 200:
            self.content = json.loads(result.content)
            self.post_retrieve()

        return status


class MBZSearch(MBZAPI):
    """
    Base class for MBZ search queries
    """
    def __init__(self, endpoint: str):
        super(MBZSearch, self).__init__(endpoint=endpoint)
        self.records: dict = None
        self.record_count: int = 0

    def __repr__(self):
        return f'<MBZSearch on {self.endpoint}>'

    def post_retrieve(self) -> None:
        """identify and count the records our search was interested in"""
        if not self.is_retrieved:
            raise ValueError('No data yet retrieved, can\'t run post-retrieve')
        self.records = self.content.get(self.endpoint + 's')
        self.record_count = self.content.get('count')
        # TODO: instantiate an object of base_class per record?

    @property
    def request_params(self):
        raise NotImplementedError


class MBZLookup(MBZAPI):
    """
    Base class for MBZ lookup queries
    """
    # TODO: figure out how to type hint a variable representing a class
    def __init__(self, endpoint: str, mbz_id: str = None, obj_class=None):
        super(MBZLookup, self).__init__(endpoint=endpoint, mbz_id=mbz_id)
        self.base_class = obj_class
        self.obj: obj_class = None

    @property
    def request_params(self) -> dict:
        return {'fmt': 'json'}

    def post_retrieve(self) -> None:
        """instantiate the appropriate class for the API response"""
        if not self.is_retrieved:
            raise ValueError('No data yet retrieved, can\'t run post-retrieve')
        if self.base_class:
            self.obj = self.base_class(**self.content)
