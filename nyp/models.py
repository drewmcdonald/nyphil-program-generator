import json
import os.path
import re
import time
import unicodedata
from datetime import datetime as dt

import requests
from fuzzywuzzy import fuzz
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

# Mix-Ins


class GetOrCreateMixin(object):
    """adapted from https://stackoverflow.com/questions/2546207"""

    @classmethod
    def get_or_create(cls, session, **kwargs):
        existing = session.query(cls).filter_by(**kwargs).first()
        if existing:
            return existing
        # noinspection PyArgumentList
        new_obj = cls(**kwargs)
        session.add(new_obj)
        session.commit()
        return new_obj


class NameLookupMixin(object):
    """mixin for easy lookups by cls.get_or_create(raw_name=LOOKUP)"""

    id = Column(Integer, primary_key=True)
    raw_name = Column(String(100), nullable=False, index=True)
    name = Column(String(100), nullable=False)

    def __init__(self, raw_name: str, *args, **kwargs):
        super(NameLookupMixin, self).__init__(*args, **kwargs)
        self.raw_name = raw_name
        self.name = self.clean_name()

    def clean_name(self) -> str:
        """Strips bracketed text and extra spaces out of composer names"""
        new = re.sub(r" \[ ?[^ ,]* ?\]", " ", self.raw_name)
        new = re.sub(r" +", " ", new)
        new = re.sub(r",$", "", new)
        new = re.sub(r" ,", ",", new)
        return new.strip()


# Lookup Tables


class Orchestra(GetOrCreateMixin, NameLookupMixin, Base):
    """Simple lookup of orchestra names"""

    __tablename__ = "orchestra"

    programs = relationship("Concert", back_populates="orchestra")

    def __repr__(self):
        return f"<Orchestra {self.id}: {self.name}>"


class EventType(GetOrCreateMixin, Base):
    """Simple lookup of event types"""

    __tablename__ = "eventtype"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False, index=True)
    category = Column(String(15))
    is_modelable = Column(Boolean)

    concerts = relationship("Concert", back_populates="eventtype")

    def __repr__(self):
        return f"<EventType {self.id}: {self.name}>"


class Venue(GetOrCreateMixin, Base):
    """Simple lookup of venues (unique to location (city) and venue (space))"""

    __tablename__ = "venue"
    __table_args__ = (Index("idx_venue_lookup", "location", "venue"),)

    id = Column(Integer, primary_key=True)
    location = Column(String(100), nullable=False)
    venue = Column(String(100), nullable=False)

    concerts = relationship("Concert", back_populates="venue")

    def __repr__(self):
        return f"<Venue {self.id}: {self.venue} in {self.location}>"


class Composer(GetOrCreateMixin, NameLookupMixin, Base):
    """List of composers by name"""

    __tablename__ = "composer"

    works = relationship("Work", back_populates="composer")
    mbz_composer = relationship("MBZComposer", uselist=False, back_populates="composer")

    def __repr__(self):
        return f"<Composer {self.id}: {self.name}>"

    def to_dict(self):
        return {"id": self.id, "name": self.name}


class Performer(GetOrCreateMixin, NameLookupMixin, Base):
    """list of featured performers, including conductors"""

    __tablename__ = "performer"

    raw_name = Column(Text(1200), nullable=False)
    name = Column(Text(1200), nullable=False)
    instrument = Column(String(600))

    program_movements = relationship(
        "ConcertSelectionPerformer", back_populates="performer"
    )

    def __repr__(self):
        return f"<Performer {self.id}: {self.name} ({self.instrument}>"


# Relationship Mapping Tables


class ConcertSelection(Base):
    """List the selections associated with each concert, along with their program order"""

    __tablename__ = "concert_selection"

    id = Column(Integer, primary_key=True)
    concert_id = Column(Integer, ForeignKey("concert.id"))
    selection_id = Column(Integer, ForeignKey("selection.id"))
    concert_order = Column(Integer, nullable=False)

    concert = relationship("Concert", back_populates="concert_selections")
    selection = relationship("Selection")

    performers = relationship(
        "ConcertSelectionPerformer", back_populates="concert_selection"
    )
    movements = relationship("ConcertSelectionMovement")

    def __repr__(self):
        return (
            f"<ConcertSelection {self.id}: {self.concert} performing {self.selection}>"
        )


class ConcertSelectionMovement(Base):
    """For each selection within a concert, keep track of the relevant movements that were played"""

    __tablename__ = "concert_selection_movement"

    id = Column(Integer, primary_key=True)
    concert_selection_id = Column(Integer, ForeignKey("concert_selection.id"))
    movement_id = Column(Integer, ForeignKey("movement.id"))

    concert_selection = relationship("ConcertSelection", back_populates="movements")
    movement = relationship("Movement")

    def __repr__(self):
        return f"<ConcertSelectionMovement {self.id}: {self.movement} in {self.concert_selection}>"


class ConcertSelectionPerformer(Base):
    """For each selection within a concert, keep track of performers"""

    __tablename__ = "concert_selection_performer"

    id = Column(Integer, primary_key=True)
    concert_selection_id = Column(Integer, ForeignKey("concert_selection.id"))
    role = Column(String(5))
    performer_id = Column(Integer, ForeignKey("performer.id"))
    performer = relationship("Performer")

    concert_selection = relationship("ConcertSelection", back_populates="performers")

    def __repr__(self):
        return f"<ConcertSelectionPerformer {self.id}: {self.performer} on {self.concert_selection}>"


# Core Tables


class Movement(GetOrCreateMixin, Base):
    __tablename__ = "movement"
    __table_args__ = (Index("idx_movement_lookup", "work_id", "name"),)

    id = Column(Integer, primary_key=True)

    work_id = Column(Integer, ForeignKey("work.id"), nullable=False)
    work_movement_id = Column(Integer, nullable=False)
    name = Column(String(200), nullable=False)

    work = relationship("Work", back_populates="movements")

    def __repr__(self):
        return f"<Movement {self.id}: {self.name} ({self.work_movement_id}), from {self.work}>"


class Work(GetOrCreateMixin, Base):
    """Ignore original work_id due to duplicates in the source data. Some works have
    different titles under the same IDs, some works have different IDs for the same titles.
    """

    __tablename__ = "work"
    __table_args__ = (Index("idx_work_lookup", "composer_id", "title"),)

    id = Column(Integer, primary_key=True)

    title = Column(String(150), nullable=False)
    composer_id = Column(Integer, ForeignKey("composer.id"), nullable=False)
    composer = relationship("Composer", back_populates="works")

    movements = relationship("Movement", back_populates="work")
    selections = relationship("Selection", back_populates="work")

    UniqueConstraint(composer_id, title)

    def __repr__(self):
        return f"<Work {self.id}: {self.title} by {self.composer}>"

    def to_dict(self):
        return {"id": self.id, "title": self.title, "composer": self.composer.to_dict()}


class Selection(GetOrCreateMixin, Base):
    __tablename__ = "selection"

    id = Column(Integer, primary_key=True)
    is_full_work = Column(Boolean, nullable=False)
    work_id = Column(Integer, ForeignKey("work.id"), nullable=False)

    work = relationship("Work", back_populates="selections")

    UniqueConstraint(work_id, is_full_work)

    def __repr__(self):
        front = "Full work of" if self.is_full_work else "Selection(s) from"
        return f"<Selection {self.id}: {front} {self.work}>"

    def to_dict(self):
        return {
            "id": self.id,
            "is_full_work": self.is_full_work,
            "work": self.work.to_dict(),
        }


class Concert(Base):
    __tablename__ = "concert"

    id = Column(Integer, primary_key=True)

    orchestra_id = Column(Integer, ForeignKey("orchestra.id"))
    venue_id = Column(Integer, ForeignKey("venue.id"))
    eventtype_id = Column(Integer, ForeignKey("eventtype.id"))

    orchestra = relationship("Orchestra")
    venue = relationship("Venue", back_populates="concerts")
    eventtype = relationship("EventType", back_populates="concerts")

    datetime = Column(DateTime, nullable=False)

    season = Column(String(10))

    concert_selections = relationship("ConcertSelection", back_populates="concert")

    def __repr__(self):
        return (
            f"<Concert {self.id}: {self.orchestra} at {self.venue} "
            f'on {self.datetime.strftime("%d/%m/%Yl")}>'
        )

    @property
    def selections(self) -> [Selection]:
        """shortcut to the actual selections instead of the concert_selection records
        (though this loses movements)
        """
        # noinspection PyTypeChecker
        return [cs.selection for cs in self.concert_selections]


class MBZAPI(object):
    """
    Base class for all MusicBrainz API contact
    """

    BASE_URL = "https://musicbrainz.org/ws/2/"
    APP_HEADERS = {
        "User-Agent": "NYPhil Concert Builder/0.01 \
                    (https://github.com/drewmcdonald/nyphil-program-generator)"
    }

    def __init__(self, endpoint: str, mbz_id: str = None):
        self.endpoint: str = endpoint
        self.mbz_id: str = mbz_id
        self.add_params: dict = {}
        self.request_status_code: int = None
        self.content: dict = None

    def __repr__(self):
        return f"<MBZAPI at endpoint {self.endpoint}>"

    @property
    def is_retrieved(self):
        return self.request_status_code == 200

    @property
    def request_url(self):
        return os.path.join(self.BASE_URL, self.endpoint, self.mbz_id or "")

    @property
    def request_params(self) -> dict:
        return {**{"fmt": "json"}, **self.add_params}

    def post_retrieve(self):
        pass

    def retrieve(self) -> int:
        """make an MBZ API Request, then call the class's post-retrieve method

        :return: request's HTTP status code"""
        time.sleep(0.3)

        result = requests.get(
            self.request_url, params=self.request_params, headers=self.APP_HEADERS
        )

        if result.status_code == 200:
            self.content = json.loads(result.content)
            self.request_status_code = 200
            self.post_retrieve()
        if result.status_code == 503:
            print("request rejected. sleeping .3 another seconds")
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
            "fmt": "json",
            "limit": 1,
            "offset": 0,
            self.index_endpoint: self.index_mbz_id,
        }

    def post_retrieve(self):
        """extract the count of the composer records"""
        count_key = self.endpoint + "-count"
        self.record_count = self.content[count_key]


class MBZArea(MBZAPI):
    """Object to represent an MBZ Area
    recursively calls parent relationships to find the area's country
    """

    def __init__(self, mbz_id: str):
        super(MBZArea, self).__init__(endpoint="area", mbz_id=mbz_id)
        self.name = None
        self.sort_name = None
        self.iso_1_code: str = None
        self.iso_2_code: str = None
        self.retrieve()
        self.recurse_parents()

    def __repr__(self):
        return f"<MBZAreaLookup for {self.name}>"

    @property
    def request_params(self) -> dict:
        return {"fmt": "json", "inc": "area-rels"}

    def post_retrieve(self):
        """Fill object attributes from the json response"""

        if not self.is_retrieved:
            raise ValueError("No data yet retrieved")

        self.name = self.content.get("name")
        self.sort_name = self.content.get("sort-name")

        iso_1_codes: list = self.content.get("iso-3166-1-codes", [])
        iso_2_codes: list = self.content.get("iso-3166-2-codes", [])

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
        if self.content.get("relations"):
            parent_rels = [
                x for x in self.content["relations"] if x.get("direction") == "backward"
            ]
        else:
            return

        # push down any iso codes found in the parent
        for parent_rel in parent_rels:
            parent_obj = MBZArea(parent_rel["area"]["id"])

            # country areas do not have regional codes, and vice versa, so we have to check
            # these each independently to avoid overwriting data from further down the hierarchy
            self.iso_1_code = self.iso_1_code or parent_obj.iso_1_code
            self.iso_2_code = self.iso_2_code or parent_obj.iso_2_code


class MBZComposer(Base):
    """
    Object representation of MBZ composer search result
    """

    __tablename__ = "mbz_composer"

    id = Column(Integer, primary_key=True)
    mbz_id = Column(String(36), nullable=False, index=True)
    composer_id = Column(Integer, ForeignKey("composer.id"))
    composer = relationship("Composer", back_populates="mbz_composer")
    score = Column(Integer)
    country = Column(String(2))
    gender = Column(String(6))
    sort_name = Column(String(50))
    area_name = Column(String(50))
    begin_area_name = Column(String(50))
    end_area_name = Column(String(50))
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
    area_iso_2_code = Column(String(6))
    begin_area_iso_1_code = Column(String(2))
    begin_area_iso_2_code = Column(String(6))
    end_area_iso_1_code = Column(String(2))
    end_area_iso_2_code = Column(String(6))

    is_best_match = Column(Boolean, default=False)

    n_works = Column(Integer)
    n_recordings = Column(Integer)
    n_releases = Column(Integer)

    def __init__(self, composer: Composer, record: dict):
        self.composer: Composer = composer
        self.parse_base_record(record)
        self.score_name_match()

    def __repr__(self):
        return f"<MBZComposer for {self.composer.name} ({self.mbz_id})>"

    @classmethod
    def clean_name(cls, name: str) -> str:
        """remove commas and unicode from a name str input"""
        clean = name.replace(",", "").replace(".", "").lower()
        norm = str(unicodedata.normalize("NFKD", clean).encode("ASCII", "ignore"))
        return norm

    @classmethod
    def format_life_span(cls, life_span):

        if not life_span:
            return None

        def do_format(format_string: str):
            return dt.date(dt.strptime(life_span, format_string))

        life_span_length = len(life_span)
        if life_span_length == 4:
            return do_format("%Y")
        if life_span_length == 7:
            return do_format("%Y-%m")
        if life_span_length == 10:
            return do_format("%Y-%m-%d")

        return None

    def parse_base_record(self, record: dict) -> None:
        """parse a composer search api json result into a flattened object"""
        self.mbz_id = record.get("id")
        self.score = int(record.get("score"))
        self.country = record.get("country")
        self.gender = record.get("gender")
        self.sort_name = record.get("sort-name")

        if record.get("area"):
            self.area_name = record.get("area").get("name")
            self.area_id = record.get("area").get("id")

        if record.get("begin-area"):
            self.begin_area_name = record.get("begin-area").get("name")
            self.begin_area_id = record.get("begin-area").get("id")

        if record.get("end-area"):
            self.end_area_name = record.get("end-area").get("name")
            self.end_area_id = record.get("end-area").get("id")

        ls = record.get("life-span")
        if ls:
            self.lifespan_begin = self.format_life_span(ls.get("begin"))
            self.lifespan_end = self.format_life_span(ls.get("end"))
            self.lifespan_ended = ls.get("ended", False)

        if record.get("aliases"):
            self.n_aliases = len(record["aliases"])

        tags = record.get("tags")
        if tags:
            self.n_tags = len(tags)
            self.tag_composer = any([t["name"] == "composer" for t in tags])

        disambiguation = record.get("disambiguation")
        if disambiguation:
            self.disambiguated_composer = "composer" in disambiguation

    def score_name_match(self) -> None:
        """score the name match on two token comparisons, as well as their average
        """
        composer_name = self.clean_name(self.composer.name)
        match_name = self.clean_name(self.sort_name)
        self.match_token_sort_ratio = fuzz.token_sort_ratio(composer_name, match_name)
        self.match_partial_ratio = fuzz.partial_ratio(composer_name, match_name)
        self.match_average_ratio = (
            self.match_partial_ratio + self.match_token_sort_ratio
        ) / 2

    def count_related_records(self, endpoint: str) -> int:
        return MBZCounter(
            endpoint, index_endpoint="artist", index_mbz_id=self.mbz_id
        ).record_count

    def fill_additional_data(self) -> None:

        # gather country and region codes from areas
        # TODO: this could be faster and cheaper with a cache of mbz_id to iso codes or a table of Areas
        if self.area_id:
            area: MBZArea = MBZArea(self.area_id)
            self.area_iso_1_code, self.area_iso_2_code = (
                area.iso_1_code,
                area.iso_2_code,
            )

        if self.begin_area_id:
            begin_area: MBZArea = MBZArea(self.begin_area_id)
            self.begin_area_iso_1_code, self.begin_area_iso_2_code = (
                begin_area.iso_1_code,
                begin_area.iso_2_code,
            )

        if self.end_area_id:
            end_area: MBZArea = MBZArea(self.end_area_id)
            self.end_area_iso_1_code, self.end_area_iso_2_code = (
                end_area.iso_1_code,
                end_area.iso_2_code,
            )

        # count number of works, number of recordings, and number of releases
        self.n_works = self.count_related_records("work")
        self.n_recordings = self.count_related_records("recording")
        self.n_releases = self.count_related_records("release")


class MBZComposerSearch(MBZAPI):
    """MBZ Composer Lookup Data
    NOTE: does not page through results. assumes a good match would be in the top 25 results"""

    def __init__(self, composer: Composer):
        super(MBZComposerSearch, self).__init__(endpoint="artist")
        self.composer: Composer = composer
        self.record_count: int = 0
        self.objects: [MBZComposer] = []
        self.best_match: MBZComposer = None
        self.retrieve()

    def __repr__(self):
        return f"<MBZComposerSearch for {self.composer.name}>"

    @property
    def request_params(self) -> dict:
        return {
            "query": "name:{} AND type:person".format(self.composer.name),
            "fmt": "json",
        }

    def post_retrieve(self) -> None:
        """identify and count the records our search was interested in;
        convert them to MBZComposer objects"""
        if not self.is_retrieved:
            raise ValueError("No data yet retrieved, can't run post-retrieve")

        self.record_count = self.content.get("count")
        self.objects = [
            MBZComposer(self.composer, r) for r in self.content.get(self.endpoint + "s")
        ]

    @classmethod
    def acceptable_match_filter(cls, match: MBZComposer) -> bool:
        """return true if both ratios are >= 70 and their average is >= 85"""
        return (
            match.match_token_sort_ratio >= 70
            and match.match_partial_ratio >= 70
            and match.match_average_ratio >= 85
        )

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
            most_aliases = list(
                filter(lambda rec: rec.n_aliases == max_n_aliases, acceptable_matches)
            )
            # if only one record with the max number of aliases, that's our best match
            if len(most_aliases) == 1:
                return exit_with_result(most_aliases[0])

        # prefer the one result that's tagged as a composer if possible
        tagged_composer = list(filter(lambda rec: rec.tag_composer, acceptable_matches))
        if len(tagged_composer) == 1:
            return exit_with_result(tagged_composer[0])

        # prefer the one result that's disambiguated as a composer if possible
        disambiguated_composer = list(
            filter(lambda rec: rec.disambiguated_composer, acceptable_matches)
        )
        if len(disambiguated_composer) == 1:
            return exit_with_result(disambiguated_composer[0])

        return exit_with_result(None)
