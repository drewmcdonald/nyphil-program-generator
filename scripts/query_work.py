from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import re

from nyp.raw.schema import (
    ConcertSelection, EventType
)
from nyp.mbz.composer import MBZComposer


def matches_any(input_string: str, patterns: list) -> str:
    """detect if input_string matches any of patterns (return 'yes'), else return 'no'"""
    for pattern in patterns:
        if re.search(pattern, input_string):
            return 'yes'
    return 'no'


def matches_which(input_string: str, patterns: dict) -> [str, None]:
    """given an input string and a dictionary of {'LABEL1': ['lab1pat1', 'lab1pat2'], 'LABEL2': ['lab2pat1', 'lab2pat2']},
    return 'LABEL1' if either 'lab1pat1' or 'lab1pat2' matches to input string, etc."""
    for pattern_name in patterns:
        for sub_pattern in patterns[pattern_name]:
            if re.search(sub_pattern, input_string):
                return pattern_name
    return None


if __name__ == '__main__':
    Session = sessionmaker(create_engine("sqlite:///../data/raw.db"))

    s = Session()
    q = s.query(ConcertSelection)\
        .filter(EventType.name == 'Subscription Season')\
        .slice(220, 230)

    for r in q.all():
        result = (r.selection.work.composer.name,
                  r.selection.is_full_work,
                  r.selection.work.title,
                  [p.performer.instrument for p in r.performers if p.performer.instrument != 'Conductor'],
                  # see https://en.wikipedia.org/wiki/Opus_number for other markers
                  matches_any(r.selection.work.title, ['BWV\. ?\d+', 'K\. ?\d+', 'OP\. ?\d+']),
                  matches_which(r.selection.work.title, {'symphony': ['SYMPHONY', 'SINFONI'],
                                                         'concerto': ['CONCERTO']}),
                  'concerto' in r.selection.work.title.lower(),
                  'symphony' in r.selection.work.title.lower())
        print(result)
