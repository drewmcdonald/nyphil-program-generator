import json

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from nyp.models import Base
from nyp.parsers import ProgramParser

engine = create_engine("sqlite:///data/raw.db")
Base.metadata.bind = engine

# Base.metadata.drop_all(engine)
# Base.metadata.create_all(engine)

Session = sessionmaker(engine)
s = Session()

with open('complete.json') as f:
    programs = json.load(f)['programs']

n_programs = len(programs)
counter = 1
for program in programs:
    if counter % 100 == 0:
        print(f'Working on program {counter}/{n_programs}')
    pp = ProgramParser(program, s)
    pp.load_relationships()
    counter += 1

s.close()
engine.dispose()
