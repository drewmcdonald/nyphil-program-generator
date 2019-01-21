from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from collections import OrderedDict
import re

from nyp.models import ConcertSelection, EventType

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


if __name__ == '__main__':
    Session = sessionmaker(create_engine("sqlite:///../data/raw.db"))

    s = Session()
    q = s.query(ConcertSelection) \
        .filter(EventType.name == 'Subscription Season') \
        .limit(10)

    work_types = OrderedDict({'symphony': ['SYMPHONY', 'SINFONI'],
                              'concerto': ['CONCERTO'],
                              'overture': ['OVERTURE']})

    for r in q.all():
        result = (r.concert_id,
                  r.selection_id,
                  matches_any(r.selection.work.title, ['BWV\. ?\d+', 'K\. ?\d+', 'OP\. ?\d+']),
                  matches_which(r.selection.work.title, work_types))
        print(result)
