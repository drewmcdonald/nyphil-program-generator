import math
import re
from collections import OrderedDict, namedtuple

from nyp.lookups import INSTRUMENT_CATEGORIES
from nyp.models import Composer

WORK_TYPES = OrderedDict(
    {
        "concerto": ["CONCI?ERTO"],
        "mass": ["MASS", "REQUIEM", "ORATORIO"],
        "dance": [
            r"DAN(S|C)E",
            "WALTZ",
            "VALSE",
            "MINUET",
            "TANGO",
            r"GALL?OP",
            "POLKA",
            "TARANTELLA",
            "BOLERO",
            "BALLET",
        ],
        "suite": ["SUITE"],
        "overture": ["INTRODUCTION", "OVERTURE"],
        "march": ["MARCH"],
        "symphony": ["SYMPHON(Y|I)"],
    }
)

OPUS_MARKERS = [r"BWV \d+", r"K\. ?\d+", r"OP\. ?\d+", r"D. ?\d+"]


def matches_any(input_string: str, patterns: list) -> str:
    """detect if input_string matches any of patterns (return 'yes'), else return 'no'"""
    for pattern in patterns:
        if re.search(pattern, input_string):
            return "yes"
    return "no"


def matches_which(input_string: str, patterns: OrderedDict) -> [str, None]:
    """given an input string and a dictionary of
    {'LABEL1': ['lab1pat1', 'lab1pat2'], 'LABEL2': ['lab2pat1', 'lab2pat2']},
    return 'LABEL1' if either 'lab1pat1' or 'lab1pat2' matches to input string, etc."""
    for pattern_name in patterns:
        for sub_pattern in patterns[pattern_name]:
            if re.search(sub_pattern, input_string):
                return pattern_name
    return "Other"


def categorize_soloists(instruments: list) -> str:
    n_features = len(instruments)

    if n_features == 0:
        return "No Featured Instruments"

    features_list = [INSTRUMENT_CATEGORIES.get(i, "Other") for i in instruments]

    if n_features == 1:
        return features_list[0]

    features_set = set(features_list)
    n_distinct_features = len(features_set)

    if n_distinct_features == 1:
        return "Multiple " + features_set.pop() + "s"

    if "Ensemble" in features_set:
        return "Ensemble+"

    return "Multiple"


def coalesce_country(composer: Composer) -> [str, None]:
    """coalesce a composer's mbz country codes if available"""
    mbz = composer.mbz_composer
    if not mbz:
        return "Unknown"
    return (
        mbz.area_iso_1_code
        or mbz.end_area_iso_1_code
        or mbz.begin_area_iso_1_code
        or "Unknown"
    )


def composer_birth_century(composer: Composer) -> [str, None]:
    mbz = composer.mbz_composer
    if not mbz:
        return "Other"
    if mbz.lifespan_begin:
        return str(math.floor(mbz.lifespan_begin.year / 100) + 1) + "th"
    return "Other"


if __name__ == "__main__":
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker, joinedload
    from nyp.models import (
        Concert,
        ConcertSelection,
        EventType,
        Selection,
        Work,
        Composer,
        ConcertSelectionPerformer,
    )
    from dotenv import load_dotenv
    from os import getenv

    load_dotenv()

    engine = create_engine(getenv("MYSQL_CON_DEV"))
    Session = sessionmaker(engine)

    s = Session()

    composer_concert_selection_counts = {
        r[0]: r[2]
        for r in engine.execute(
            "select * from composer_concert_selection_counts;"
        ).fetchall()
    }
    selection_performance_counts = {
        r[0]: {"n_performances": r[1], "n_performances_grp": r[2]}
        for r in engine.execute(
            "select * from selection_performance_counts;"
        ).fetchall()
    }
    selection_position_stats = {
        r[0]: {
            "percent_after_intermission_bin": r[1],
            "avg_percent_of_concert_bin": r[2],
        }
        for r in engine.execute("select * from selection_position_stats;").fetchall()
    }

    q = (
        s.query(ConcertSelection)
        .join(Concert)
        .join(EventType)
        .filter(EventType.is_modelable)
        .options(
            joinedload(ConcertSelection.selection, innerjoin=True)
            .joinedload(Selection.work, innerjoin=True)
            .joinedload(Work.composer, innerjoin=True)
            .joinedload(Composer.mbz_composer, innerjoin=False),
            joinedload(ConcertSelection.performers, innerjoin=False).joinedload(
                ConcertSelectionPerformer.performer, innerjoin=True
            ),
        )
    )

    Row = namedtuple(
        "Row",
        [
            "concert_id",
            "selection_id",
            "weight",
            "full_work",
            "has_opus",
            "is_arrangement",
            "work_type",
            "composer_country",
            "composer_birth_century",
            "composer_concert_selections",
            "soloist_type",
            "selection_performances",
            "percent_after_intermission_bin",
            "avg_percent_of_concert_bin",
        ],
    )
    data_list = []
    i = 0
    for r in q.all():
        if i % 1000 == 0:
            print(i)
        if r.selection_id == 4:
            result = Row(
                r.concert_id,
                r.selection_id,
                selection_performance_counts[r.selection_id]["n_performances"],
                "___INTERMISSION__",
                "___INTERMISSION__",
                "___INTERMISSION__",
                "___INTERMISSION__",
                "___INTERMISSION__",
                "___INTERMISSION__",
                "___INTERMISSION__",
                "___INTERMISSION__",
                "___INTERMISSION__",
                "___INTERMISSION__",
                "___INTERMISSION__",
            )
        else:
            result = Row(
                r.concert_id,
                r.selection_id,
                selection_performance_counts[r.selection_id]["n_performances"],
                "Full Work" if r.selection.is_full_work else "Selections",
                matches_any(r.selection.work.title, OPUS_MARKERS),
                matches_any(r.selection.work.title, [r"ARR\."]),
                matches_which(r.selection.work.title, WORK_TYPES),
                coalesce_country(r.selection.work.composer),
                composer_birth_century(r.selection.work.composer),
                composer_concert_selection_counts[r.selection.work.composer.id],
                categorize_soloists(
                    [
                        x.performer.instrument
                        for x in r.performers
                        if x.role == "S" and x.performer.instrument != "Conductor"
                    ]
                ),
                selection_performance_counts[r.selection_id]["n_performances_grp"],
                selection_position_stats[r.selection_id][
                    "percent_after_intermission_bin"
                ],
                selection_position_stats[r.selection_id]["avg_percent_of_concert_bin"],
            )
        data_list.append(result)
        i += 1

    import pandas as pd

    df = pd.DataFrame(data_list)
    df = df.set_index(["concert_id", "selection_id"])
    df.to_csv("../data/train_export.txt.gz", sep="\t", index=True, compression="gzip")
