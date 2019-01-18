import unicodedata
from fuzzywuzzy import fuzz
from datetime import datetime as dt

from nyp.raw.schema import Composer
from .api import MBZAPI, MBZCounter, MBZArea


class MBZComposer(object):
    """
    Object representation of MBZ composer search result
    """
    # TODO: convert to sqlalchemy table
    def __init__(self, target: Composer, record: dict):
        self.target: Composer = target
        self.mbz_id: str = None
        self.score: int = None
        self.country: str = None
        self.gender: str = None
        self.sort_name: str = None
        self.area_name: str = None
        self.begin_area_name: str = None
        self.end_area_name: str = None
        self.area_id: str = None
        self.begin_area_id: str = None
        self.end_area_id: str = None
        self.lifespan_begin: dt.date = None
        self.lifespan_end: dt.date = None
        self.lifespan_ended: bool = None
        self.n_aliases: int = 0
        self.n_tags: int = 0
        self.disambiguated_composer: bool = False
        self.tag_composer: bool = False

        self.parse_base_record(record)

        self.match_token_sort_ratio: float = None
        self.match_partial_ratio: float = None
        self.match_average_ratio: float = None

        self.score_name_match()

        # these cost additional API calls, so we'll trigger them
        # in the calling code only if this record is the best passing match
        self.area_iso_1_code: str = None
        self.area_iso_2_code: str = None
        self.begin_area_iso_1_code: str = None
        self.begin_area_iso_2_code: str = None
        self.end_area_iso_1_code: str = None
        self.end_area_iso_2_code: str = None

        self.n_works: int = None
        self.n_recordings: int = None
        self.n_releases: int = None

    def __repr__(self):
        return f'<MBZComposer for {self.target.name} ({self.mbz_id})>'

    @classmethod
    def clean_name(cls, name: str) -> str:
        """remove commas and unicode from a name str input"""
        clean = name.replace(',', '').lower()
        norm = str(unicodedata.normalize('NFKD', clean).encode('ASCII', 'ignore'))
        return norm

    def parse_base_record(self, record: dict) -> None:
        """parse a composer search api json result into a flattened object"""
        self.mbz_id = record.get('id')
        self.score = int(record.get('score'))
        self.country = record.get('country')
        self.gender = record.get('gender')
        self.sort_name = record.get('sort-name')

        if record.get('area'):
            self.area_name = record.get('area').get('name')
            self.area_id = record.get('area').get('id')

        if record.get('begin-area'):
            self.begin_area_name = record.get('begin-area').get('name')
            self.begin_area_id = record.get('begin-area').get('id')

        if record.get('end-area'):
            self.end_area_name = record.get('end-area').get('name')
            self.end_area_id = record.get('end-area').get('id')

        ls = record.get('life-span')
        if ls:
            ls_begin = ls.get('begin')
            if ls_begin:
                ls_begin_len = len(ls_begin)
                ls_begin_fmt = '%Y' if ls_begin_len == 4 else '%Y-%m-%d'
                self.lifespan_begin = dt.date(dt.strptime(ls_begin, ls_begin_fmt))

            ls_end = ls.get('end')
            if ls_end:
                ls_end_len = len(ls_end)
                ls_end_fmt = '%Y' if ls_end_len == 4 else '%Y-%m-%d'
                self.lifespan_end = dt.date(dt.strptime(ls_end, ls_end_fmt))

            self.lifespan_ended = ls.get('ended')

        if record.get('aliases'):
            self.n_aliases = len(record['aliases'])

        tags = record.get('tags')
        if tags:
            self.n_tags = len(tags)
            self.tag_composer = any([t['name'] == 'composer' for t in tags])

        disambiguation = record.get('disambiguation')
        if disambiguation:
            self.disambiguated_composer = 'composer' in disambiguation

    def score_name_match(self) -> str:
        """score the name match on two token comparisons, as well as their average
        """
        composer_name = self.clean_name(self.target.name)
        match_name = self.clean_name(self.sort_name)
        self.match_token_sort_ratio = fuzz.token_sort_ratio(composer_name, match_name)
        self.match_partial_ratio = fuzz.partial_ratio(composer_name, match_name)
        self.match_average_ratio = (self.match_partial_ratio + self.match_token_sort_ratio) / 2

    def count_related_records(self, endpoint: str) -> int:
        return MBZCounter(endpoint, index_endpoint='artist', index_mbz_id=self.mbz_id).record_count

    def fill_additional_data(self) -> None:
        # gather country and region codes from areas
        if self.area_id:
            area: MBZArea = MBZArea(self.area_id)
            self.area_iso_1_code, self.area_iso_2_code = area.iso_1_code, area.iso_2_code

        if self.begin_area_id:
            begin_area: MBZArea = MBZArea(self.begin_area_id)
            self.begin_area_iso_1_code, self.begin_area_iso_2_code = begin_area.iso_1_code, begin_area.iso_2_code

        if self.end_area_id:
            end_area: MBZArea = MBZArea(self.end_area_id)
            self.end_area_iso_1_code, self.end_area_iso_2_code = end_area.iso_1_code, end_area.iso_2_code

        # count number of works, number of recordings, and number of releases
        self.n_works = self.count_related_records('work')
        self.n_recordings = self.count_related_records('recording')
        self.n_releases = self.count_related_records('release')


class MBZComposerSearch(MBZAPI):
    """MBZ Composer Lookup Data"""

    def __init__(self, composer: Composer):
        super(MBZComposerSearch, self).__init__(endpoint='artist')
        self.composer: Composer = composer
        self.record_count: int = 0
        self.objects: [MBZComposer] = []
        self.best_match: MBZComposer = None
        self.retrieve()

    def __repr__(self):
        return f'<MBZComposerSearch for {self.composer.name}>'

    @property
    def request_params(self) -> dict:
        return {
            'query': 'name:{} AND type:person'.format(self.composer.name),
            'fmt': 'json'
        }

    def post_retrieve(self) -> None:
        """identify and count the records our search was interested in;
        convert them to MBZComposer objects"""
        if not self.is_retrieved:
            raise ValueError('No data yet retrieved, can\'t run post-retrieve')

        self.record_count = self.content.get('count')
        self.objects = [MBZComposer(self.composer, r) for r in self.content.get(self.endpoint + 's')]

    @classmethod
    def acceptable_match_filter(cls, match: MBZComposer) -> bool:
        """return true if both ratios are >= 70 and their average is >= 80"""
        return match.match_token_sort_ratio >= 70 and\
            match.match_partial_ratio >= 70 and\
            match.match_average_ratio >= 80

    def pick_best_match(self) -> MBZComposer:
        """pick the best match among mbz results; first by token comparison scores
        then by whoever has the most aliases (most famous)
        then by whoever has a composer tag
        then by whoever has 'composer' in their disambiguation
        """
        # TODO: check this logic
        # filter by token comparison score
        acceptable_matches = list(filter(self.acceptable_match_filter, self.objects))
        if len(acceptable_matches) == 1:
            result = acceptable_matches[0]
        else:
            max_n_aliases = max([rec.n_aliases for rec in acceptable_matches])
            most_aliases = list(filter(lambda rec: rec.n_aliases == max_n_aliases, acceptable_matches))
            if len(most_aliases) == 1:
                result = most_aliases[0]
            else:
                # tagged composer and disambiguated composer will both just use the most_aliases set
                tagged_composer = list(filter(lambda rec: rec.tag_composer, most_aliases))
                if len(tagged_composer) == 1:
                    result = tagged_composer[0]
                else:
                    disambiguated_composer = list(filter(lambda rec: rec.disambiguated_composer, most_aliases))
                    if len(disambiguated_composer) == 1:
                        result = disambiguated_composer[0]
                    else:
                        result = None

        if result:
            result.fill_additional_data()

        self.best_match = result

        return result
