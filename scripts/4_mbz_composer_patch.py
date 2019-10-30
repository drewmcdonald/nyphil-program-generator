import json
from collections import namedtuple

from nyp.models import Composer, MBZComposer
from nyp.musicbrainz import MBZAPI
from nyp.util import engine, wrapped_session

# export for manual lookups
# select
#   c.name, c.id, count(w.id) as n_works
# from composer c
#   inner join work w on c.id=w.composer_id
#   left join mbz_composer m on c.id=m.composer_id
# where m.composer_id is null
# group by 1, 2 order by 3 desc;

Record = namedtuple("Row", ("name", "id", "n_works", "mbz_id"))

with open("data/manual_musicbrainz_composer_ids.json", "r") as fp:
    composer_data = [
        Record(**record) for record in json.load(fp) if record.get("mbz_id")
    ]


class MBZArtistExtras(MBZAPI):
    endpoint = "artist"

    @property
    def add_params(self):
        return {"inc": "aliases+ratings+tags"}


for row in composer_data:

    with wrapped_session() as s:

        existing_mbz_composer = (
            s.query(MBZComposer).filter_by(mbz_id=row.mbz_id).first()
        )
        if existing_mbz_composer:
            continue

        # do this lookup by name in case the composer ID changes between runs
        c = s.query(Composer).filter(Composer.name == row.name).first()

        # use the low level API class to get the raw content needed for the MBZComposer constructor
        x = MBZArtistExtras(mbz_id=row.mbz_id)
        x.retrieve()
        x.content["score"] = 100  # fake the search score

        m = MBZComposer(c, x.content)
        m.is_best_match = True
        m.fill_additional_data(s)
        s.add(m)

engine.dispose()
