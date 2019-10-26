# noinspection PyPackageRequirements
from os import getenv

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import joinedload, sessionmaker

from nyp.models import (
    Base,
    Composer,
    Concert,
    ConcertSelection,
    ConcertSelectionMovement,
    ConcertSelectionPerformer,
    Selection,
    Work,
)

load_dotenv()

engine1 = create_engine("sqlite:////Users/drew/Desktop/nyp/data/raw.db", echo=True)
engine2 = create_engine(
    f'mysql+pymysql://{getenv("MYSQL_USER")}:{getenv("MYSQL_PASS")}@{getenv("MYSQL_HOST")}/{getenv("MYSQL_DB")}'
)

Session_sqlite = sessionmaker(engine1)
Session_mysql = sessionmaker(engine2)


def main():
    s_lite = Session_sqlite()
    s_my = Session_mysql()
    # Base.metadata.drop_all(engine2)
    # Base.metadata.create_all(engine2)
    i = 0
    q = s_lite.query(ConcertSelection).options(
        joinedload(ConcertSelection.concert, innerjoin=True).joinedload(
            Concert.orchestra, innerjoin=True
        ),
        joinedload(ConcertSelection.concert, innerjoin=True).joinedload(
            Concert.eventtype, innerjoin=True
        ),
        joinedload(ConcertSelection.concert, innerjoin=True).joinedload(
            Concert.venue, innerjoin=True
        ),
        joinedload(ConcertSelection.selection, innerjoin=True)
        .joinedload(Selection.work, innerjoin=True)
        .joinedload(Work.composer, innerjoin=True)
        .joinedload(Composer.mbz_composer, innerjoin=False),
        joinedload(ConcertSelection.movements, innerjoin=False).joinedload(
            ConcertSelectionMovement.movement, innerjoin=True
        ),
        joinedload(ConcertSelection.performers, innerjoin=False).joinedload(
            ConcertSelectionPerformer.performer, innerjoin=True
        ),
    )

    n = s_lite.query(ConcertSelection).count()
    for cs in q.all():

        if i % 1000 == 0:
            print(f"{i:07,} / {n:07,}")

        s_my.merge(cs)

        i += 1

        s_my.commit()


if __name__ == "__main__":
    main()
