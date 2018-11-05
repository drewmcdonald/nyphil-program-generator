from sqlalchemy import create_engine, Column, String, Integer, DateTime, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import sessionmaker, relationship

from datetime import datetime as dt


from mixins import GetOrCreateMixin, Base
from lookup_tables import Orchestra, EventType, Composer, Venue, Conductor, Soloist


# Relationship Tables

class ProgramMovement(Base):
    __tablename__ = 'program_movement'

    id = Column(Integer, primary_key=True)
    program_id = Column(Integer, ForeignKey('program.id'))
    movement_id = Column(Integer, ForeignKey('movement.id'))
    conductor_id = Column(Integer, ForeignKey('conductor.id'))

    program = relationship('Program', back_populates='program_movements')
    movement = relationship('Movement', back_populates='programs')
    conductor = relationship('Conductor', back_populates='program_movements')

    soloists = relationship('ProgramMovementSoloist', back_populates='program_movement')

    def __repr__(self):
        return f'<ProgramMovement {self.id}: {self.movement} conducted by {self.conductor}>'


class ProgramMovementSoloist(Base):
    __tablename__ = 'program_movement_soloist'

    id = Column(Integer, primary_key=True)
    pm_id = Column(Integer, ForeignKey('program_movement.id'))
    role = Column(String(1))
    soloist_id = Column(Integer, ForeignKey('soloist.id'))
    soloist = relationship('Soloist')

    program_movement = relationship('ProgramMovement', back_populates='soloists')

    def __repr__(self):
        return f'<ProgramMovementSoloist {self.id}: {self.soloist} performing {self.program_movement}>'


# Main Classes

class Program(Base):
    __tablename__ = 'program'

    id = Column(Integer, primary_key=True)
    guid = Column(String)
    season = Column(String)
    orchestra_id = Column(Integer, ForeignKey('orchestra.id'))

    orchestra = relationship('Orchestra', back_populates='programs')
    concerts = relationship('Concert', back_populates='program')

    program_movements = relationship('ProgramMovement', back_populates='program')

    def __repr__(self):
        return f'<Program {self.id} from {self.season}>'

    @classmethod
    def from_json_data(cls, s, data):
        program_id = int(data.get('programID'))
        existing_program = s.query(Program).get(program_id)
        if existing_program is not None:
            print(f'Program {program_id} is duplicated')
            return existing_program

        guid = data.get('id')
        season = data.get('season')
        program = Program(id=program_id, guid=guid, season=season)
        s.add(program)

        orchestra_name = data.get('orchestra', 'No Orchestra')
        program.orchestra = Orchestra.get_or_create(s, raw_name=orchestra_name)

        for concert_data in data['concerts']:
            new_concert = Concert.from_jsondata(s, concert_data)
            program.concerts.append(new_concert)

        # what the NYPhil calls 'works' are actually movements of works, each with a conductor and set of soloists
        for movement_data in data['works']:

            movement = Movement.from_jsondata(s, movement_data)
            conductor = Conductor.get_or_create(s, raw_name=movement_data.get('conductorName', 'No Conductor'))

            program_movement = ProgramMovement(program=program, movement=movement, conductor=conductor)
            s.add(program_movement)

            # now loop through movement soloists; has to be done with program awareness
            for soloist_data in movement_data.get('soloists'):
                soloist_name = soloist_data.get('soloistName')
                soloist_inst = soloist_data.get('soloistInstrument')
                soloist_role = soloist_data.get('soloistRoles')
                soloist = Soloist.get_or_create(s, raw_name=soloist_name, instrument=soloist_inst)

                program_movement_soloist = ProgramMovementSoloist(role=soloist_role, soloist=soloist,
                                                                  program_movement=program_movement)
                s.add(program_movement_soloist)

        s.add(program)
        s.commit()
        return program


class Concert(Base):
    __tablename__ = 'concert'

    id = Column(Integer, primary_key=True)
    datetime = Column(DateTime, nullable=False)

    venue_id = Column(Integer, ForeignKey('venue.id'))
    program_id = Column(Integer, ForeignKey('program.id'))
    eventtype_id = Column(Integer, ForeignKey('eventtype.id'))

    venue = relationship('Venue', back_populates='concerts')
    program = relationship('Program', back_populates='concerts')
    eventtype = relationship('EventType', back_populates='concerts')

    def __repr__(self):
        return f'<Concert {self.id}: {self.datetime.strftime("%x")}'

    @classmethod
    def from_jsondata(cls, s, data):

        concert_date = data.get('Date')[0:10]
        concert_time = data.get('Time')
        if concert_time == 'None':
            concert_time = '12:00AM'
        concert_datetime = dt.strptime(f"{concert_date} {concert_time}", '%Y-%m-%d %I:%M%p')

        venue = Venue.get_or_create(s, location=data.get('Location'), venue=data.get('Venue'))

        event_type = EventType.get_or_create(s, name=data.get('eventType'))

        con = Concert(datetime=concert_datetime, venue=venue, eventtype=event_type)

        return con


class Movement(GetOrCreateMixin, Base):
    __tablename__ = 'movement'

    __table_args__ = (Index('idx_movement_lookup', 'orig_id', 'work_id', 'work_movement_id', 'name'), )

    id = Column(Integer, primary_key=True)

    orig_id = Column(String, nullable=False)
    work_id = Column(Integer, ForeignKey('work.id'), nullable=False)
    work_movement_id = Column(Integer, nullable=False)
    name = Column(String, nullable=False)

    work = relationship('Work', back_populates='movements')

    programs = relationship('ProgramMovement', back_populates='movement')

    def __repr__(self):
        return f'<Movement {self.id} ({self.work_movement_id}): {self.name}, from {self.work}>'

    @classmethod
    def split_orig_id(cls, orig_id):
        s = orig_id.split('*')
        work_id = int(s[0])
        work_movement_id = 0 if s[1] == '' else int(s[1])
        return work_id, work_movement_id

    @classmethod
    def process_title(cls, title):
        # sometimes it's a string, sometimes its a dict. fucking stupid
        if type(title) is dict:
            title = f"{title.get('_', '')} {title.get('em', '')}".strip()
        return title

    @classmethod
    def from_jsondata(cls, s, data):

        orig_id = data.get('ID')
        work_id, work_movement_id = cls.split_orig_id(orig_id)

        # handle the work first
        composer_name = data.get('composerName', 'No Composer')
        composer = Composer.get_or_create(session, raw_name=composer_name)

        work_title = cls.process_title(data.get('workTitle'))
        if work_title is None:
            work_title = data.get('interval')

        work = Work.retrieve_existing_or_create(s, work_id, composer.id, work_title)

        # movement data
        movement_name = data.get('movement')
        if movement_name is None:
            if work_movement_id == 0:
                movement_name = 'Full Work'
            else:
                movement_name = f'Movement {work_movement_id}'

        movement_name = cls.process_title(movement_name)

        movement = Movement.get_or_create(s, orig_id=orig_id, work_id=work.id,
                                          work_movement_id=work_movement_id, name=movement_name)

        s.add(movement)
        s.commit()
        return movement


class Work(Base):
    __tablename__ = 'work'

    __table_args__ = (Index('idx_work_lookup', 'composer_id', 'title'),)

    id = Column(Integer, primary_key=True)

    title = Column(String, nullable=False)
    composer_id = Column(Integer, ForeignKey('composer.id'), nullable=False)
    composer = relationship('Composer', back_populates='works')

    movements = relationship('Movement', back_populates='work')

    UniqueConstraint(composer_id, title)

    def __repr__(self):
        return f'<Work {self.id}: {self.title} by {self.composer}>'

    @classmethod
    def retrieve_existing_or_create(cls, s, work_id: int, composer_id: int, work_title: str):
        """This method is necessary because of duplicates in the source data. Some works have different titles under
        the same IDs, some works have different IDs for the same titles. Where possible, we'll keep the IDs the same to
        maintain some utility with Movement's orig_id
        """
        # if id exists, just take that no matter what
        existing_by_id = s.query(Work).get(work_id)
        if existing_by_id is not None:
            return existing_by_id

        existing_by_feature = s.query(Work).filter_by(composer_id=composer_id, title=work_title).first()
        if existing_by_feature is not None:
            return existing_by_feature

        new_work = Work(id=work_id, title=work_title, composer_id=composer_id)
        s.add(new_work)
        s.commit()

        return new_work


if __name__ == "__main__":
    import json

    engine = create_engine("sqlite:///db.db")
    # engine = create_engine("postgresql://nyphil:nyphil@192.168.1.20:5432/postgres")
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(engine)

    session = Session()

    with open('complete.json') as f:
        programs = json.load(f)['programs']

    for p in programs:
        Program.from_json_data(session, p)

    p = session.query(Program).first()
    for w in p.program_movements:
        print(w)

    session.close()
    engine.dispose()
