from datetime import datetime as dt
from typing import List, Optional, Tuple, Union

from sqlalchemy.orm import Session

from .models import (
    Composer,
    Concert,
    ConcertSelection,
    ConcertSelectionMovement,
    ConcertSelectionPerformer,
    EventType,
    Movement,
    Orchestra,
    Performer,
    Selection,
    Venue,
    Work,
)


def clean_name_obj(name_obj):
    """TODO: this needs to strip multiple spaces as well"""
    if type(name_obj) is dict:
        return f"{name_obj.get('_', '').strip()} {name_obj.get('em', '').strip()}"
    return name_obj


def parse_concert_datetime(date_str, time_str) -> dt:
    date_str = date_str[0:10]
    if time_str == "None":
        time_str = "12:00AM"
    return dt.strptime(f"{date_str} {time_str}", "%Y-%m-%d %I:%M%p")


class SoloistParser:
    def __init__(self, raw: dict, session: Session):
        self.name = raw.get("soloistName")
        self.instrument = raw.get("soloistInstrument")
        self.role = raw.get("soloistRoles")
        self.performer: Performer = Performer.get_or_create(
            session, raw_name=self.name, instrument=self.instrument
        )


class WorkParser:
    def __init__(self, raw: dict, session: Session):
        self.raw = raw
        self.session = session
        self.composer = Composer.get_or_create(
            session, raw_name=raw.get("composerName", "No Composer")
        )

        self.work = self.get_or_create_work()
        self.movement = self.get_or_create_movement()
        self.selection = self.get_or_create_selection()
        self.performers = self.set_performers()

    def __repr__(self):
        return f"<WorkParser for {self.work} movement {self.movement}>"

    def get_or_create_work(self) -> Work:
        title = clean_name_obj(self.raw.get("workTitle", self.raw.get("interval")))
        return Work.get_or_create(
            self.session, composer_id=self.composer.id, title=title
        )

    def get_or_create_movement(self) -> Union[Movement, None]:
        name = self.raw.get("movement")
        if name is None:
            return None

        work_movement_id = self.raw.get("ID", "*").split("*")[1]
        work_movement_id = 0 if work_movement_id == "" else int(work_movement_id)

        return Movement.get_or_create(
            self.session,
            work_id=self.work.id,
            work_movement_id=work_movement_id,
            name=clean_name_obj(name),
        )

    def get_or_create_selection(self) -> Selection:
        return Selection.get_or_create(
            self.session, work_id=self.work.id, is_full_work=self.movement is None
        )

    def set_performers(self) -> List[Tuple[Performer, Optional[str]]]:
        performers: List[Tuple[Performer, Optional[str]]] = []
        conductor_name = self.raw.get("conductorName")
        soloists = self.raw.get("soloists", [])

        if conductor_name:
            conductor = Performer.get_or_create(
                self.session, instrument="Conductor", raw_name=conductor_name
            )
            performers.append((conductor, "C"))

        for soloist in soloists:
            soloist_name = soloist.get("soloistName")
            if soloist_name == "" or soloist_name is None:
                continue
            soloist_parser = SoloistParser(soloist, self.session)
            performers.append((soloist_parser.performer, soloist_parser.role))

        return performers


class ConcertParser:
    def __init__(self, raw: dict, session: Session):
        self.raw = raw
        self.session = session
        self.datetime = parse_concert_datetime(raw.get("Date"), raw.get("Time"))
        self.venue = Venue.get_or_create(
            session, location=raw.get("Location"), venue=raw.get("Venue")
        )
        self.event_type = EventType.get_or_create(session, name=raw.get("eventType"))

    def __repr__(self):
        return f"<ConcertParser for concert on {self.datetime}>"

    def new_concert_record(self, season, orchestra) -> Concert:
        c = Concert(
            season=season,
            orchestra=orchestra,
            venue=self.venue,
            event_type=self.event_type,
            datetime=self.datetime,
        )
        self.session.add(c)
        self.session.commit()
        return c


class ProgramParser:
    def __init__(self, raw: dict, session: Session):
        self.raw: dict = raw
        self.session = session
        self.guid = raw.get("id")
        self.season = raw.get("season")
        self.orchestra = Orchestra.get_or_create(
            session, raw_name=raw.get("orchestra", "- No Orchestra -")
        )
        self.concerts = self.parse_concerts()
        self.works = [WorkParser(w_data, session) for w_data in raw.get("works", [])]

    def __repr__(self):
        return f"<ProgramParser for program {self.guid}>"

    def parse_concerts(self) -> List[Concert]:
        return [
            ConcertParser(c_data, self.session).new_concert_record(
                self.season, self.orchestra
            )
            for c_data in self.raw.get("concerts", [])
        ]

    def load_relationships(self):

        for c in self.concerts:
            concert_id = c.id
            concert_ord = 0
            last_concert_selection = None

            for work in self.works:

                selection_id = work.selection.id
                if (
                    last_concert_selection is not None
                    and selection_id == last_concert_selection.selection_id
                ):
                    cs = last_concert_selection
                else:
                    concert_ord += 1
                    cs = ConcertSelection(
                        concert_id=concert_id,
                        selection_id=selection_id,
                        concert_order=concert_ord,
                    )

                # add concert selection performers
                cs.performers = [
                    ConcertSelectionPerformer(role=role, performer=performer)
                    for performer, role in work.performers
                ]

                # add concert selection movements
                if work.movement:
                    csm = ConcertSelectionMovement(
                        movement_id=work.movement.id, concert_selection=cs
                    )
                    cs.movements.append(csm)

                self.session.add(cs)
                self.session.commit()

                last_concert_selection = cs
