import unicodedata
from fuzzywuzzy import fuzz
from datetime import datetime as dt

from nyp.raw.schema import Composer
from .api import MBZAPI, MBZCounter, MBZArea

from sqlalchemy import Column, String, Integer, Date, Boolean, Float, ForeignKey
from sqlalchemy.orm import relationship

from nyp.raw.schema import Base


class MBZComposer(Base):
    """
    Object representation of MBZ composer search result
    """

    __tablename__ = 'mbz_composer'
    id = Column(Integer, primary_key=True)
    mbz_id = Column(String(36), nullable=False, index=True)
    composer_id = Column(Integer, ForeignKey('composer.id'))
    composer = relationship('Composer', back_populates='mbz_composer')
    score = Column(Integer)
    country = Column(String)
    gender = Column(String)
    sort_name = Column(String)
    area_name = Column(String)
    begin_area_name = Column(String)
    end_area_name = Column(String)
    area_id = Column(String(36))
    begin_area_id = Column(String(36))
    end_area_id = Column(String(36))
    lifespan_begin = Column(Date)
    lifespan_end = Column(Date)
    lifespan_ended = Column(Boolean, default=False)

    n_aliases = Column(Integer, default=0)
    n_tags = Column(Integer, default=0)
    disambiguated_composer = Column(Boolean, default=False)
    tag_composer = Column(Boolean, default=False)
    match_token_sort_ratio = Column(Float)
    match_partial_ratio = Column(Float)
    match_average_ratio = Column(Float)
    area_iso_1_code = Column(String(2))
    area_iso_2_code = Column(String(5))
    begin_area_iso_1_code = Column(String(2))
    begin_area_iso_2_code = Column(String(5))
    end_area_iso_1_code = Column(String(2))
    end_area_iso_2_code = Column(String(5))

    is_best_match = Column(Boolean, default=False)

    n_works = Column(Integer)
    n_recordings = Column(Integer)
    n_releases = Column(Integer)

    def __init__(self, composer: Composer, record: dict):
        self.composer: Composer = composer
        self.parse_base_record(record)
        self.score_name_match()

    def __repr__(self):
        return f'<MBZComposer for {self.composer.name} ({self.mbz_id})>'

    @classmethod
    def clean_name(cls, name: str) -> str:
        """remove commas and unicode from a name str input"""
        clean = name.replace(',', '').replace('.', '').lower()
        norm = str(unicodedata.normalize('NFKD', clean).encode('ASCII', 'ignore'))
        return norm

    @classmethod
    def format_life_span(cls, life_span):

        if not life_span:
            return None

        def do_format(format_string: str):
            return dt.date(dt.strptime(life_span, format_string))

        life_span_length = len(life_span)
        if life_span_length == 4:
            return do_format('%Y')
        if life_span_length == 7:
            return do_format("%Y-%m")
        if life_span_length == 10:
            return do_format("%Y-%m-%d")

        return None

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
            self.lifespan_begin = self.format_life_span(ls.get('begin'))
            self.lifespan_end = self.format_life_span(ls.get('end'))
            self.lifespan_ended = ls.get('ended', False)

        if record.get('aliases'):
            self.n_aliases = len(record['aliases'])

        tags = record.get('tags')
        if tags:
            self.n_tags = len(tags)
            self.tag_composer = any([t['name'] == 'composer' for t in tags])

        disambiguation = record.get('disambiguation')
        if disambiguation:
            self.disambiguated_composer = 'composer' in disambiguation

    def score_name_match(self) -> None:
        """score the name match on two token comparisons, as well as their average
        """
        composer_name = self.clean_name(self.composer.name)
        match_name = self.clean_name(self.sort_name)
        self.match_token_sort_ratio = fuzz.token_sort_ratio(composer_name, match_name)
        self.match_partial_ratio = fuzz.partial_ratio(composer_name, match_name)
        self.match_average_ratio = (self.match_partial_ratio + self.match_token_sort_ratio) / 2

    def count_related_records(self, endpoint: str) -> int:
        return MBZCounter(endpoint, index_endpoint='artist', index_mbz_id=self.mbz_id).record_count

    def fill_additional_data(self) -> None:

        # gather country and region codes from areas
        # TODO: this could be faster and cheaper with a cache of mbz_id to iso codes or a table of Areas
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
    """MBZ Composer Lookup Data
    NOTE: does not page through results. assumes if a good match isn't in the top 25 results, it doesn't exist"""

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
        """return true if both ratios are >= 70 and their average is >= 85"""
        return match.match_token_sort_ratio >= 70 and\
            match.match_partial_ratio >= 70 and\
            match.match_average_ratio >= 85

    def pick_best_match(self) -> MBZComposer:
        """pick the best match among mbz results; first by token comparison scores
        then by whoever has the most aliases (most famous)
        then by whoever has a composer tag
        then by whoever has 'composer' in their disambiguation
        """

        def exit_with_result(final_result):
            """helper to finalize the result once we find it"""
            if final_result:
                final_result.fill_additional_data()
                final_result.is_best_match = True
            self.best_match = final_result
            return final_result

        # filter to acceptable matches by token comparison scores
        acceptable_matches = list(filter(self.acceptable_match_filter, self.objects))

        # Take the match if there is only one; give up if there are none
        if len(acceptable_matches) == 1:
            return exit_with_result(acceptable_matches[0])
        if len(acceptable_matches) == 0:
            return exit_with_result(None)

        # list of all records' number of aliases
        all_n_aliases = [rec.n_aliases for rec in acceptable_matches if rec.n_aliases]
        # if any aliases at all, find the maximum number and filter to records with that number of aliases
        if len(all_n_aliases) > 0:
            max_n_aliases = max(all_n_aliases)
            most_aliases = list(filter(lambda rec: rec.n_aliases == max_n_aliases, acceptable_matches))
            # if only one record with the max number of aliases, that's our best match
            if len(most_aliases) == 1:
                return exit_with_result(most_aliases[0])

        # prefer the one result that's tagged as a composer if possible
        tagged_composer = list(filter(lambda rec: rec.tag_composer, acceptable_matches))
        if len(tagged_composer) == 1:
            return exit_with_result(tagged_composer[0])

        # prefer the one result that's disambiguated as a composer if possible
        disambiguated_composer = list(filter(lambda rec: rec.disambiguated_composer, acceptable_matches))
        if len(disambiguated_composer) == 1:
            return exit_with_result(disambiguated_composer[0])

        return exit_with_result(None)
