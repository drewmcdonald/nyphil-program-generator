from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from nyp.raw.schema import (
    ConcertSelection, EventType
)


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
                  'concerto' in r.selection.work.title.lower(),
                  'symphony' in r.selection.work.title.lower())
        print(result)
