from sqlalchemy import Column, String, Integer, Index
from sqlalchemy.orm import relationship

from mixins import GetOrCreateMixin, NameLookupMixin, Base


class Orchestra(GetOrCreateMixin, NameLookupMixin, Base):
    """Simple lookup of orchestra names"""
    __tablename__ = 'orchestra'

    programs = relationship('Program', back_populates='orchestra')

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
    __tablename__ = 'composer'

    works = relationship('Work', back_populates='composer')

    def __repr__(self):
        return f'<Composer {self.id}: {self.name}>'


class Conductor(GetOrCreateMixin, NameLookupMixin, Base):
    __tablename__ = 'conductor'

    program_movements = relationship('ProgramMovement', back_populates='conductor')

    def __repr__(self):
        return f'<Conductor {self.id}: {self.name}>'


class Soloist(GetOrCreateMixin, NameLookupMixin, Base):
    __tablename__ = 'soloist'

    instrument = Column(String)

    program_movements = relationship('ProgramMovementSoloist', back_populates='soloist')

    def __repr__(self):
        return f'<Soloist {self.id}: {self.name} ({self.instrument}>'
