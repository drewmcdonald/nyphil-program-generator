from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import pandas as pd

from nyp.raw.schema import Composer, MBZComposer
from nyp.mbz.api import MBZAPI


production_data = 'data/raw.db'
engine = create_engine(f"sqlite:///{production_data}", echo=False)

Session = sessionmaker(engine)
s = Session()

# delete non-best-matches from mbz_composer
# delete from mbz_composer where not is_best_match;

# export for manual lookups to composers_unmatched.txt
# select
#   c.name, c.id, count(w.id) as n_works
# from composer c
#   inner join work w on c.id=w.composer_id
#   left join mbz_composer m on c.id=m.composer_id
# where m.composer_id is null
# group by 1, 2 order by 3 desc;

df = pd.read_table('data/composers_unmatched.txt')
df = df.loc[pd.notna(df.mbz_id), ['id', 'mbz_id']]

for r in df.itertuples():

    c = s.query(Composer).get(r.id)
    if c.mbz_composer:
        continue

    print(c)

    # use the low level API class to get the raw content needed for the MBZComposer constructor
    x = MBZAPI(endpoint='artist', mbz_id=r.mbz_id)
    x.add_params = {'inc': 'aliases+ratings+tags'}
    x.retrieve()
    x.content['score'] = 100  # fake the search score

    m = MBZComposer(c, x.content)
    m.is_best_match = True
    m.fill_additional_data()

    s.add(m)
    s.commit()
