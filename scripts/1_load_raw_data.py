import gzip
import json

from nyp.config import LOCAL_RAW_DATA_FILE
from nyp.models import Base
from nyp.parsers import ProgramParser
from nyp.util import engine, wrapped_session

with gzip.open(LOCAL_RAW_DATA_FILE) as f:
    programs = json.load(f)["programs"]

n_programs = len(programs)

Base.metadata.bind = engine
# Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)


for i, program in enumerate(programs, start=1):
    if i % 100 == 0:
        print(f"Working on program {i}/{n_programs}")
    with wrapped_session() as s:
        pp = ProgramParser(program, s)
        pp.load_relationships()

engine.dispose()
