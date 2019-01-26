
from datetime import datetime as dt
from .models import (
    Orchestra, EventType, Composer, Venue, Performer,
    Concert, ConcertSelection, ConcertSelectionMovement, 
    ConcertSelectionPerformer, Movement, Work, Selection
)

from sqlalchemy.orm import Session

from typing import List, Union, Tuple


class SoloistParser(object):

    def __init__(self, raw: dict, session: Session):
        self.raw = raw
        self.session = session
        self.name, self.instrument, self.role = self.set_basics()
        self.performer = self.get_performer()

    def set_basics(self) -> Tuple[str, str, str]:
        return self.raw.get('soloistName'), \
               self.raw.get('soloistInstrument'), \
               self.raw.get('soloistRoles')

    def get_performer(self) -> Performer:
        return Performer.get_or_create(self.session, raw_name=self.name, instrument=self.instrument)


class WorkParser(object):

    def __init__(self, raw: dict, session: Session):
        self.raw = raw
        self.session = session
        self.composer = self.set_composer()
        self.work = self.get_or_create_work()
        self.movement = self.get_or_create_movement()
        self.selection = self.get_or_create_selection()
        self.performers = self.set_performers()

    def __repr__(self):
        return f"<WorkParser for {self.work} movement {self.movement}>"

    def set_composer(self) -> Composer:
        return Composer.get_or_create(self.session, raw_name=self.raw.get('composerName', 'No Composer'))

    def parse_conductor(self) -> Union[Performer, None]:
        conductor_name = self.raw.get('conductorName')
        if conductor_name is None:
            return None
        return Performer.get_or_create(self.session, instrument='Conductor', raw_name=conductor_name)

    def parse_soloists(self) -> List[Tuple[Performer, str]]:
        soloist_performers = []
        for s_data in self.raw.get('soloists'):
            s_parser = SoloistParser(s_data, self.session)
            soloist_performers.append((s_parser.performer, s_parser.role))
        return soloist_performers

    def set_performers(self) -> List[Tuple[Performer, str]]:
        performers = []
        conductor = self.parse_conductor()
        if conductor:
            performers.append((conductor, 'C'))
        performers += self.parse_soloists()
        return performers

    @classmethod
    def clean_name(cls, name):
        """TODO: this needs to strip multiple spaces as well"""
        if type(name) is dict:
            return f"{name.get('_', '').strip()} {name.get('em', '').strip()}"
        return name

    def get_or_create_work(self) -> Work:
        composer_id = self.composer.id
        title = self.raw.get('workTitle')
        if title is None:
            title = self.raw.get('interval')
        title = self.clean_name(title)

        return Work.get_or_create(self.session, composer_id=composer_id, title=title)

    def get_or_create_movement(self) -> Union[Movement, None]:
        name = self.raw.get('movement')
        if name is None:
            return None
        name = self.clean_name(name)

        work_movement_id = self.raw.get('ID').split('*')[1]
        work_movement_id = 0 if work_movement_id == '' else int(work_movement_id)

        return Movement.get_or_create(self.session, work_id=self.work.id,
                                      work_movement_id=work_movement_id, name=name)

    def get_or_create_selection(self) -> Selection:
        is_full_work = self.movement is None
        return Selection.get_or_create(self.session, work_id=self.work.id, is_full_work=is_full_work)


class ConcertParser(object):

    def __init__(self, raw: dict, session: Session):
        self.raw = raw
        # self.program_data = program_data
        self.session = session
        self.datetime = self.parse_concert_datetime()
        self.venue = self.set_venue()
        self.eventtype = self.set_eventtype()

    def __repr__(self):
        return f'<ConcertParser for concert on {self.datetime}>'

    def new_concert_record(self, season, orchestra) -> Concert:
        c = Concert(season=season, orchestra=orchestra, venue=self.venue,
                    eventtype=self.eventtype, datetime=self.datetime)
        self.session.add(c)
        self.session.commit()
        return c

    def parse_concert_datetime(self) -> dt:
        concert_date = self.raw.get('Date')[0:10]
        concert_time = self.raw.get('Time')
        if concert_time == 'None':
            concert_time = '12:00AM'
        return dt.strptime(f"{concert_date} {concert_time}", '%Y-%m-%d %I:%M%p')

    def set_venue(self) -> Venue:
        return Venue.get_or_create(self.session,
                                   location=self.raw.get('Location'),
                                   venue=self.raw.get('Venue'))

    def set_eventtype(self) -> EventType:
        return EventType.get_or_create(self.session, name=self.raw.get('eventType'))


class ProgramParser(object):

    def __init__(self, raw: dict, session: Session):
        self.raw: dict = raw
        self.session = session
        self.guid = raw.get('id')
        self.season = raw.get('season')
        self.orchestra = self.set_orchestra()
        self.concerts = self.parse_concerts()
        self.works = self.load_works()

    def __repr__(self):
        return f'<ProgramParser for program {self.guid}>'

    def set_orchestra(self) -> Orchestra:
        return Orchestra.get_or_create(self.session,
                                       raw_name=self.raw.get('orchestra', '- No Orchestra -'))

    def parse_concerts(self) -> List[Concert]:
        concerts = []
        for c_data in self.raw.get('concerts'):
            c_parser = ConcertParser(c_data, self.session)
            c = c_parser.new_concert_record(season=self.season, orchestra=self.orchestra)
            concerts.append(c)

        return concerts

    def load_works(self) -> List[WorkParser]:
        works = []
        for w_data in self.raw.get('works'):
            w_parser = WorkParser(w_data, self.session)
            works.append(w_parser)
        return works

    def load_relationships(self) -> bool:

        for c in self.concerts:
            c_id = c.id

            concert_ord = 0
            last_cs = None

            for w in self.works:

                s_id = w.selection.id

                # add concert selection if not already existing
                if last_cs is not None and s_id == last_cs.selection_id:
                    cs = last_cs
                else:
                    concert_ord += 1
                    cs = ConcertSelection(concert_id=c_id, selection_id=s_id, concert_order=concert_ord)

                # add concert selection performers
                for p, role in w.performers:
                    csp = ConcertSelectionPerformer(role=role, performer=p)
                    cs.performers += [csp]

                # add concert selection movements
                if w.movement:
                    csm = ConcertSelectionMovement(movement_id=w.movement.id, concert_selection=cs)
                    cs.movements += [csm]

                # sync relationships
                self.session.add(cs)
                self.session.commit()

                last_cs = cs

        return True
