from nyp.models import Base, Composer, MBZComposerSearch
from nyp.util import Session, engine, wrapped_session

Base.metadata.bind = engine

composer_ids = (
    Session()
    .query(Composer.id)
    .filter(Composer.name != "No Composer", ~Composer.mbz_composer.has())
    .all()
)

for composer_id in composer_ids:
    with wrapped_session() as s:
        composer = s.query(Composer).get(composer_id)
        search = MBZComposerSearch(composer=composer)
        if search.best_match:
            search.best_match.is_best_match = True
            search.best_match.fill_additional_data(s)
            s.add(search.best_match)

engine.dispose()
