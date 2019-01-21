from collections import OrderedDict, namedtuple
import re


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
    if len(instruments) == 0:
        return 'No Instruments'
    if 'Piano' in instruments:
        return 'Keyboard'
    if 'Harpsichord' in instruments:
        return 'Keyboard'
    if any(i in instruments for i in ['Violin', 'Viola', 'Cello', 'Double Bass']):
        return 'String'
    if any(i in instruments for i in ['Soprano', 'Mezzo-Soprano', 'Countertenor',
                                      'Tenor', 'Baritone', 'Bass']):
        return 'String'

    return 'Other'


if __name__ == '__main__':
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from nyp.models import Concert, ConcertSelection, EventType

    Session = sessionmaker(create_engine("sqlite:///../data/raw.db", echo=False))

    s = Session()
    q = s.query(ConcertSelection) \
        .join(Concert)\
        .join(EventType)\
        .filter(EventType.is_modelable) \
        .limit(10)

    work_types = OrderedDict({'symphony': ['SYMPHONY', 'SINFONI'],
                              'concerto': ['CONCERTO'],
                              'overture': ['OVERTURE'],
                              'march': ['MARCH']})

    opus_markers = ['BWV \d+', 'K\. ?\d+', 'OP\. ?\d+']

    for r in q.all():
        result = (r.concert_id,
                  r.selection_id,
                  matches_any(r.selection.work.title, opus_markers),
                  matches_which(r.selection.work.title, work_types),
                  categorize_soloists([x.performer.instrument for x in r.performers if x.performer.instrument != 'Conductor']),
                  [x.performer.instrument for x in r.performers if x.performer.instrument != 'Conductor']
                  )
        print(result)
