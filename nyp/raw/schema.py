from sqlalchemy import Column, String, Integer, DateTime, Boolean, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

import re


Base = declarative_base()

# Mix-Ins


class GetOrCreateMixin(object):
    """adapted from https://stackoverflow.com/questions/2546207"""

    @classmethod
    def get_or_create(cls, session, **kwargs):
        existing = session.query(cls).filter_by(**kwargs).first()
        if existing:
            return existing
        # noinspection PyArgumentList
        new_obj = cls(**kwargs)
        session.add(new_obj)
        session.commit()
        return new_obj


class NameLookupMixin(object):
    """mixin for easy lookups by cls.get_or_create(raw_name=LOOKUP)"""

    id = Column(Integer, primary_key=True)
    raw_name = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False)

    def __init__(self, raw_name: str, *args, **kwargs):
        super(NameLookupMixin, self).__init__(*args, **kwargs)
        self.raw_name = raw_name
        self.name = self.clean_name()

    def clean_name(self) -> str:
        """Strips bracketed text and extra spaces out of composer names"""
        new = re.sub(r' \[ ?[^ ,]* ?\]', ' ', self.raw_name)
        new = re.sub(r' +', ' ', new)
        new = re.sub(r',$', '', new)
        new = re.sub(r' ,', ',', new)
        return new.strip()


# Lookup Tables


class Orchestra(GetOrCreateMixin, NameLookupMixin, Base):
    """Simple lookup of orchestra names"""
    __tablename__ = 'orchestra'

    programs = relationship('Concert', back_populates='orchestra')

    def __repr__(self):
        return f'<Orchestra {self.id}: {self.name}>'


class EventType(GetOrCreateMixin, Base):
    """Simple lookup of event types"""
    __tablename__ = 'eventtype'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, index=True)

    concerts = relationship('Concert', back_populates='eventtype')

    def __repr__(self):
        return f'<EventType {self.id}: {self.name}>'


class Venue(GetOrCreateMixin, Base):
    """Simple lookup of venues (unique to location (city) and venue (space))"""
    __tablename__ = 'venue'
    __table_args__ = (Index('idx_venue_lookup', 'location', 'venue'),)

    id = Column(Integer, primary_key=True)
    location = Column(String, nullable=False)
    venue = Column(String, nullable=False)

    concerts = relationship('Concert', back_populates='venue')

    def __repr__(self):
        return f'<Venue {self.id}: {self.venue} in {self.location}>'


class Composer(GetOrCreateMixin, NameLookupMixin, Base):
    """List of composers by name"""
    __tablename__ = 'composer'

    works = relationship('Work', back_populates='composer')
    mbz_composer = relationship('MBZComposer', uselist=False, back_populates='composer')

    def __repr__(self):
        return f'<Composer {self.id}: {self.name}>'


class Performer(GetOrCreateMixin, NameLookupMixin, Base):
    """list of featured performers, including conductors"""
    __tablename__ = 'performer'

    instrument = Column(String)

    program_movements = relationship('ConcertSelectionPerformer', back_populates='performer')

    def __repr__(self):
        return f'<Performer {self.id}: {self.name} ({self.instrument}>'


# Relationship Mapping Tables


class ConcertSelection(Base):
    """List the selections associated with each concert, along with their program order"""
    __tablename__ = 'concert_selection'

    id = Column(Integer, primary_key=True)
    concert_id = Column(Integer, ForeignKey('concert.id'))
    selection_id = Column(Integer, ForeignKey('selection.id'))
    concert_order = Column(Integer, nullable=False)

    concert = relationship('Concert', back_populates='concert_selections')
    selection = relationship('Selection')

    performers = relationship('ConcertSelectionPerformer', back_populates='concert_selection')
    movements = relationship('ConcertSelectionMovement')

    def __repr__(self):
        return f'<ConcertSelection {self.id}: {self.concert} performing {self.selection}>'


class ConcertSelectionMovement(Base):
    """For each selection within a concert, keep track of the relevant movements that were played"""
    __tablename__ = 'concert_selection_movement'

    id = Column(Integer, primary_key=True)
    concert_selection_id = Column(Integer, ForeignKey('concert_selection.id'))
    movement_id = Column(Integer, ForeignKey('movement.id'))

    concert_selection = relationship('ConcertSelection', back_populates='movements')
    movement = relationship('Movement')

    def __repr__(self):
        return f'<ConcertSelectionMovement {self.id}: {self.movement} in {self.concert_selection}>'


class ConcertSelectionPerformer(Base):
    """For each selection within a concert, keep track of performers"""
    __tablename__ = 'concert_selection_performer'

    id = Column(Integer, primary_key=True)
    concert_selection_id = Column(Integer, ForeignKey('concert_selection.id'))
    role = Column(String(1))
    performer_id = Column(Integer, ForeignKey('performer.id'))
    performer = relationship('Performer')

    concert_selection = relationship('ConcertSelection', back_populates='performers')

    def __repr__(self):
        return f'<ConcertSelectionPerformer {self.id}: {self.performer} on {self.concert_selection}>'


# Core Tables

class Movement(GetOrCreateMixin, Base):
    __tablename__ = 'movement'
    __table_args__ = (Index('idx_movement_lookup', 'work_id', 'name'), )

    id = Column(Integer, primary_key=True)

    work_id = Column(Integer, ForeignKey('work.id'), nullable=False)
    work_movement_id = Column(Integer, nullable=False)
    name = Column(String, nullable=False)

    work = relationship('Work', back_populates='movements')

    def __repr__(self):
        return f'<Movement {self.id}: {self.name} ({self.work_movement_id}), from {self.work}>'


class Work(GetOrCreateMixin, Base):
    """Ignore original work_id due to duplicates in the source data. Some works have
    different titles under the same IDs, some works have different IDs for the same titles.
    """
    __tablename__ = 'work'
    __table_args__ = (Index('idx_work_lookup', 'composer_id', 'title'),)

    id = Column(Integer, primary_key=True)

    title = Column(String, nullable=False)
    composer_id = Column(Integer, ForeignKey('composer.id'), nullable=False)
    composer = relationship('Composer', back_populates='works')

    movements = relationship('Movement', back_populates='work')
    selections = relationship('Selection', back_populates='work')

    UniqueConstraint(composer_id, title)

    def __repr__(self):
        return f'<Work {self.id}: {self.title} by {self.composer}>'


class Selection(GetOrCreateMixin, Base):
    __tablename__ = 'selection'

    id = Column(Integer, primary_key=True)
    is_full_work = Column(Boolean, nullable=False)
    work_id = Column(Integer, ForeignKey('work.id'), nullable=False)

    work = relationship('Work', back_populates='selections')

    UniqueConstraint(work_id, is_full_work)

    def __repr__(self):
        front = "Full work of" if self.is_full_work else "Selection(s) from"
        return f'<Selection {self.id}: {front} {self.work}>'


class Concert(Base):
    __tablename__ = 'concert'

    id = Column(Integer, primary_key=True)

    orchestra_id = Column(Integer, ForeignKey('orchestra.id'))
    venue_id = Column(Integer, ForeignKey('venue.id'))
    eventtype_id = Column(Integer, ForeignKey('eventtype.id'))

    orchestra = relationship('Orchestra')
    venue = relationship('Venue', back_populates='concerts')
    eventtype = relationship('EventType', back_populates='concerts')

    datetime = Column(DateTime, nullable=False)

    season = Column(String)

    concert_selections = relationship('ConcertSelection', back_populates='concert')

    def __repr__(self):
        return f'<Concert {self.id}: {self.orchestra} at {self.venue} on {self.datetime.strftime("%d/%m/%Yl")}>'

    @property
    def selections(self) -> [Selection]:
        """shortcut to the actual selections instead of the concert_selection records (though this loses movements)"""
        # noinspection PyTypeChecker
        return [cs.selection for cs in self.concert_selections]
