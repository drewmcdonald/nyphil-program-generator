from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String

import re

Base = declarative_base()


class GetOrCreateMixin(object):
    """From https://stackoverflow.com/questions/2546207"""
    @classmethod
    def get_or_create(cls, session, **kwargs):
        existing = session.query(cls).filter_by(**kwargs).first()
        if existing:
            return existing
        newobj = cls(**kwargs)
        session.add(newobj)
        session.commit()
        return newobj


class NameLookupMixin(object):

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
