import requests
import json
import os.path
import time


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
        return os.path.join(self.BASE_URL, self.endpoint, self.mbz_id or '')

    @property
    def request_params(self) -> dict:
        return {'fmt': 'json'}

    def post_retrieve(self):
        pass

    def retrieve(self) -> int:
        """make an MBZ API Request, then call the class's post-retrieve method

        :return: request's HTTP status code"""
        time.sleep(.3)

        result = requests.get(self.request_url, params=self.request_params, headers=self.APP_HEADERS)

        if result.status_code == 200:
            self.content = json.loads(result.content)
            self.request_status_code = 200
            self.post_retrieve()
        if result.status_code == 503:
            print('request rejected. sleeping .3 another seconds')
            self.retrieve()

        return result.status_code


class MBZCounter(MBZAPI):
    """Base class for counting the number of records of type 'endpoint' affiliated with
    'index_mbz_id' of type 'index_endpoint'
    """
    def __init__(self, endpoint: str, index_endpoint: str, index_mbz_id: str):
        super(MBZCounter, self).__init__(endpoint=endpoint)
        self.index_endpoint: str = index_endpoint
        self.index_mbz_id: str = index_mbz_id
        self.record_count: int = None
        self.retrieve()

    @property
    def request_params(self) -> dict:
        return {
            'fmt': 'json',
            'limit': 1,
            'offset': 0,
            self.index_endpoint: self.index_mbz_id
        }

    def post_retrieve(self):
        """extract the count of the composer records"""
        count_key = self.endpoint + '-count'
        self.record_count = self.content[count_key]


class MBZArea(MBZAPI):
    """Object to represent an MBZ Area
    recursively calls parent relationships to find the area's country
    """

    def __init__(self, mbz_id: str):
        super(MBZArea, self).__init__(endpoint='area', mbz_id=mbz_id)
        self.name = None
        self.sort_name = None
        self.iso_1_code: str = None
        self.iso_2_code: str = None
        self.retrieve()
        self.recurse_parents()

    def __repr__(self):
        return f'<MBZAreaLookup for {self.name}>'

    @property
    def request_params(self) -> dict:
        return {'fmt': 'json', 'inc': 'area-rels'}

    def post_retrieve(self):
        """Fill object attributes from the json response"""

        if not self.is_retrieved:
            raise ValueError('No data yet retrieved')

        self.name = self.content.get('name')
        self.sort_name = self.content.get('sort-name')

        iso_1_codes: list = self.content.get('iso-3166-1-codes', [])
        iso_2_codes: list = self.content.get('iso-3166-2-codes', [])

        if len(iso_1_codes) > 0:
            self.iso_1_code = iso_1_codes[0]
        if len(iso_2_codes) > 0:
            self.iso_2_code = iso_2_codes[0]

    def recurse_parents(self):
        """go up the chain of parent areas until we can fill out
        regional and country codes
        """
        # stop if we already have a country code
        if self.iso_1_code:
            return

        # filter to up-hierarchy relationships
        if self.content.get('relations'):
            parent_rels = [x for x in self.content['relations'] if x.get('direction') == 'backward']
        else:
            return

        # push down any iso codes found in the parent
        for parent_rel in parent_rels:
            parent_obj = MBZArea(parent_rel['area']['id'])

            # country areas do not have regional codes, and vice versa, so we have to check
            # these each independently to avoid overwriting data from further down the hierarchy
            self.iso_1_code = self.iso_1_code or parent_obj.iso_1_code
            self.iso_2_code = self.iso_2_code or parent_obj.iso_2_code
