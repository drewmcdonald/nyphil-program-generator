from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from nyp.raw.schema import Composer
from nyp.mbz.composer import Base, MBZComposerSearch

from shutil import copy2
from os import remove
from os.path import isfile

production_data = 'data/raw.db'
test_data = 'data/raw_test.db'

this_data = test_data

if isfile(test_data):
    remove(test_data)

copy2(production_data, test_data)

engine = create_engine(f"sqlite:///{test_data}", echo=False)
Base.metadata.bind = engine

Session = sessionmaker(engine)
s = Session()
Base.metadata.create_all()

for c in s.query(Composer).filter(Composer.id != 4).order_by(func.random()).limit(5):
    print(c)
    search = MBZComposerSearch(c)
    search.pick_best_match()
    match = search.best_match
    if match:
        s.add(match)
        s.commit()
