from collections import OrderedDict, namedtuple
from nyp.lookups import INSTRUMENT_CATEGORIES
from nyp.models import Composer
import re
import math


def matches_any(input_string: str, patterns: list) -> str:
    """detect if input_string matches any of patterns (return 'yes'), else return 'no'"""
    for pattern in patterns:
        if re.search(pattern, input_string):
            return 'yes'
    return 'no'


def matches_which(input_string: str, patterns: OrderedDict) -> [str, None]:
    """given an input string and a dictionary of
    {'LABEL1': ['lab1pat1', 'lab1pat2'], 'LABEL2': ['lab2pat1', 'lab2pat2']},
    return 'LABEL1' if either 'lab1pat1' or 'lab1pat2' matches to input string, etc."""
    for pattern_name in patterns:
        for sub_pattern in patterns[pattern_name]:
            if re.search(sub_pattern, input_string):
                return pattern_name
    return None


def categorize_soloists(instruments: list) -> str:
    n_features = len(instruments)

    if n_features == 0:
        return 'No Featured Instruments'

    features_list = [INSTRUMENT_CATEGORIES.get(i, 'Other') for i in instruments]

    if n_features == 1:
        return features_list[0]

    features_set = set(features_list)
    n_distinct_features = len(features_set)

    if n_distinct_features == 1:
        return "Multiple " + features_set.pop() + "s"

    if 'Ensemble' in features_set:
        return "Ensemble+"

    return "Multiple"


def coalesce_country(composer: Composer) -> [str, None]:
    """coalesce a composer's mbz country codes if available"""
    mbz = composer.mbz_composer
    if not mbz:
        return None
    return mbz.area_iso_1_code or mbz.end_area_iso_1_code or mbz.begin_area_iso_1_code


def composer_death_century(composer: Composer) -> [str, None]:
    mbz = composer.mbz_composer
    if not mbz:
        return None
    if mbz.lifespan_end:
        return math.floor(mbz.lifespan_end.year / 100) + 1
    return None


if __name__ == '__main__':
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker, joinedload
    from nyp.models import Concert, ConcertSelection, EventType, Selection, Work, \
        Composer, MBZComposer, ConcertSelectionPerformer, Performer

    Session = sessionmaker(create_engine("sqlite:///../data/raw.db", echo=False))

    s = Session()

    q = s.query(ConcertSelection) \
        .join(Concert) \
        .join(EventType) \
        .filter(EventType.is_modelable) \
        .options(joinedload(ConcertSelection.selection, innerjoin=True)
                 .joinedload(Selection.work, innerjoin=True)
                 .joinedload(Work.composer, innerjoin=True)
                 .joinedload(Composer.mbz_composer, innerjoin=False),
                 joinedload(ConcertSelection.performers, innerjoin=False)
                 .joinedload(ConcertSelectionPerformer.performer, innerjoin=True)
                 )

    work_types = OrderedDict({'symphony': ['SYMPHON(Y|I)'],
                              'concerto': ['CONCI?ERTO'],
                              'mass': ['MASS', 'REQUIEM'],
                              'dance': [r'DAN(S|C)E', 'WALTZ', 'VALSE', 'MINUET', 'TANGO', r'GALL?OP', 'POLKA',
                                        'TARANTELLA', 'BOLERO', 'BALLET'],
                              'suite': ['SUITE'],
                              'overture': ['OVERTURE'],
                              'march': ['MARCH']})

    opus_markers = [r'BWV \d+', r'K\. ?\d+', r'OP\. ?\d+']

    Row = namedtuple('Row', ['concert_id', 'selection_id', 'has_opus', 'is_arrangement', 'work_type',
                             'composer_country', 'composer_death_century', 'soloist_type'])
    data_list = []
    i = 0
    for r in q.all():
        if i % 1000 == 0:
            print(i)
        result = Row(
            r.concert_id,
            r.selection_id,
            matches_any(r.selection.work.title, opus_markers),
            matches_any(r.selection.work.title, [r'ARR\.']),
            matches_which(r.selection.work.title, work_types),
            coalesce_country(r.selection.work.composer),
            composer_death_century(r.selection.work.composer),
            categorize_soloists([x.performer.instrument for x in r.performers
                                 if x.role == 'S' and x.performer.instrument != 'Conductor'])
        )
        data_list.append(result)
        i += 1
