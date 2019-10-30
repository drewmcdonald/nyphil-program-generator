from nyp.util import engine, wrapped_session

with wrapped_session() as s:
    with open("sql/0_drop_trailing_intermissions.sql") as f:
        s.execute(f.read())

engine.dispose()
