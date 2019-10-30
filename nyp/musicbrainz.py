import json
import os.path
import time
from typing import Optional

import requests

MBZ_APP_HEADER = {
    "User-Agent": "NYPhil Concert Builder/0.01 (https://github.com/drewmcdonald/nyphil-program-generator)"
}


class MBZAPI:
    """
    Base class for all MusicBrainz API contact
    """

    endpoint: str
    base_url = "https://musicbrainz.org/ws/2/"
    base_params = {"fmt": "json"}

    def __init__(self, mbz_id: str = ""):
        self.mbz_id: str = mbz_id
        self.request_status_code: Optional[int] = None
        self.content: dict = {}

    def __repr__(self):
        return f"<{self.__class__.__name__}>"

    @property
    def add_params(self):
        return {}

    def post_retrieve(self):
        if self.request_status_code != 200:
            raise ValueError(
                f"Cannot run post-retrieve for {self}. Request returned {self.request_status_code}."
            )

    def retrieve(self) -> int:
        """make an MBZ API Request, then call the class's post-retrieve method

        :return: request's HTTP status code"""
        time.sleep(0.3)

        result = requests.get(
            os.path.join(self.base_url, self.endpoint, self.mbz_id),
            params={**self.base_params, **self.add_params},
            headers=MBZ_APP_HEADER,
        )

        if result.status_code == 200:
            self.content = json.loads(result.content)
            self.request_status_code = 200
            self.post_retrieve()
        if result.status_code == 503:
            print("request rejected. sleeping another .3 seconds")
            self.retrieve()

        return result.status_code


class MBZCounter(MBZAPI):
    """Base class for counting the number of records of type 'endpoint' affiliated with
    'index_mbz_id' of type 'index_endpoint'
    """

    def __init__(self, count_endpoint: str, index_endpoint: str, index_mbz_id: str):
        super().__init__()
        self.endpoint: str = count_endpoint
        self.index_endpoint: str = index_endpoint
        self.index_mbz_id: str = index_mbz_id
        self.record_count: int = 0
        self.retrieve()

    @property
    def add_params(self):
        return {"limit": 1, "offset": 0, self.index_endpoint: self.index_mbz_id}

    def post_retrieve(self):
        """extract the count of the composer records"""
        self.record_count = self.content[self.endpoint + "-count"]
