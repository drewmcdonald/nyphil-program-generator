from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from nyp.raw.schema import Base, Composer
from nyp.musicbrainz import MBZComposerSearch

# from shutil import copy2
# from os import remove
# from os.path import isfile

# test_data = 'data/raw_test.db'
# if isfile(test_data):
#     remove(test_data)
# copy2(production_data, test_data)

production_data = 'data/raw.db'

engine = create_engine(f"sqlite:///{production_data}", echo=False)
Base.metadata.bind = engine
Base.metadata.create_all()

Session = sessionmaker(engine)
s = Session()

for c in s.query(Composer).filter(Composer.id != 4):  # no intermission
    print(c)
    search = MBZComposerSearch(c)
    search.pick_best_match()
    match = search.best_match
    if match:
        s.add(match)
        s.commit()

