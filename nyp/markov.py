from collections import defaultdict
from multiprocessing import Pool, cpu_count
from typing import Optional, Union

import numpy as np
import pandas as pd

BREAK = "___BREAK__"
MINOR = "___MINOR__"
INTERMISSION = "___INTERMISSION__"


def _internal_defaultdict_int():
    """module level function to make our defaultdict code pickle-able"""
    return defaultdict(int)


def simple_weighted_avg(p: np.ndarray, w: np.ndarray) -> np.ndarray:
    return np.sum(p * w, axis=1) / w.sum()


def sum_weighted_log_odds(p: np.ndarray, w: np.ndarray) -> np.ndarray:
    """calculate weighted odds by taking a weighted sum in log-odds space,
    then convert back to probability"""
    weighted_odds = np.exp(np.sum(np.log(p / (1 - p)) * w, axis=1) / w.sum())
    return weighted_odds / (1 + weighted_odds)


def rescaled_power_weight(p: np.ndarray, w: np.ndarray) -> np.ndarray:
    """for each row-like array of p, p0,0^w0 * p0,1^w1 ... * p0,x^wx"""
    return np.power(np.prod(p ** w, axis=1), (1 / w.sum()))


AVAILABLE_SUMMARY_FUNCTIONS = {
    "simple_weighted_avg": simple_weighted_avg,
    "sum_weighted_log_odds": sum_weighted_log_odds,
    "rescaled_power_weight": rescaled_power_weight,
}


class Chain:
    """
    build a Markov Chain from a categorical Series

    - data: a pandas Series with an index that defines the unit of analysis (usually a concert)
    - state_size: MC depth (default 1)
    - train_backwards: fit chain in reverse program order (recommended)
    - cull: impute an 'other' value in place of values with very little usage (default True)
    - cull_threshold: the floor below which a value is replaced with OTHER; interpreted as a percentage of
         total records if between 0 and .999; otherwise interpreted as a count of appearances if greater
         than or equal to 1 (default .01)
    """

    def __init__(
        self,
        data: pd.Series,
        state_size: int = 1,
        train_backwards: bool = True,
        cull: bool = True,
        cull_threshold: Union[int, float] = 0.01,
    ):
        self.name: str = data.name or "unnamed"
        self.state_size: int = state_size
        self.train_backwards: bool = train_backwards
        self.cull: bool = cull
        self.cull_threshold: Union[int, float] = cull_threshold

        self.minor_values: list = []

        new_data = data.copy()  # don't modify in place
        self.data: pd.Series = self.pre_process_data(new_data)
        self.counts: dict = self.fill_counts()
        self.probas: dict = {
            k: self.counts_to_probabilities(v) for k, v in self.counts.items()
        }

    def __repr__(self):
        return f"<Chain ({self.name}): {{{self.sample_data_str}}}>"

    @property
    def sample_data_str(self) -> str:
        """string of first few chain items"""
        if len(self.data) > 4:
            return ", ".join(self.data[:4].tolist() + ["..."])
        return ", ".join(self.data)

    @classmethod
    def from_tuple(cls, arg_tuple):
        return cls(arg_tuple[0], **arg_tuple[1])

    def pre_process_data(self, data: pd.Series) -> pd.Series:
        """pre-process the data (including culling) into a clean character vector"""

        # initialize a receptacle and break values
        break_values = [BREAK] * self.state_size
        raw_values = break_values.copy()

        for i, d in data.groupby(level=0):
            raw_values += d.values.tolist() + break_values

        values = pd.Series(raw_values)

        if self.cull:

            if self.cull_threshold < 0:
                raise ValueError("cull_threshold should be >= 0")

            summary = values[values != BREAK].value_counts()

            # convert summary to percentages if needed
            if 0 < self.cull_threshold < 1:
                summary = summary / sum(summary)

            self.minor_values = summary.index[summary < self.cull_threshold].values

            values[values.isin(self.minor_values)] = MINOR

        if self.train_backwards:
            values = values[::-1]

        return values

    @classmethod
    def counts_to_probabilities(cls, counts: dict) -> dict:
        total = sum(counts.values())
        return {c: counts[c] / total for c in counts}

    def fill_counts(self) -> dict:
        """collapse the categorical vector into a dict of probability lookups
        loosely adapted from jsvine/markovify; major difference is that this is set to accept a full corpus
        all at once with interpolated beginning/end markers
        """
        lookup: defaultdict = defaultdict(_internal_defaultdict_int)

        # accumulate counts of each input to output
        for i in range(len(self.data) - self.state_size):
            slice_end = i + self.state_size
            in_val = tuple(self.data.iloc[i:slice_end])
            out_val = self.data.iloc[i + self.state_size]

            lookup[in_val][out_val] += 1

        return lookup

    def get_probas(self, in_val: tuple):
        """fetch the probabilities for a given input"""

        if len(in_val) != self.state_size:
            raise ValueError("Input value length does not equal Chain state size")

        # replace minor values with placeholder
        in_val = tuple(i if i not in self.minor_values else MINOR for i in in_val)

        # this has to be explicit since self.probas is now a default dict
        if in_val not in self.probas:
            raise ValueError(
                f"Value {in_val} is not keyed in chain data for {self.name} chain"
            )

        return self.probas[in_val]

    def transform_scoring_series(self, data: pd.Series) -> pd.Series:
        """transform the values in the scoring series to accommodate culled minor value substitution"""
        data = data.copy()
        data.loc[data.isin(self.minor_values)] = MINOR
        return data

    def score_series(self, new_data: pd.Series, in_val: tuple) -> pd.Series:
        """apply the modeled scores to a new series of data"""

        # set up a series indexed by its own values
        new_data = pd.Series(new_data.values, index=new_data.values, name=new_data.name)

        probas = self.get_probas(in_val)
        counts = new_data.value_counts()

        # divide each probability by the number of scoring cases
        probas = {k: v / counts[k] for k, v in probas.items() if k in counts}

        # use our probas as a lookup, else 0 if that doesn't fill it
        new_data = new_data.map(probas)
        new_data.fillna(0, inplace=True)

        return new_data


class ChainEnsemble:
    def __init__(
        self, chain_configs: dict, base_chain_config: dict, train_backwards: bool = True
    ):
        self.train_backwards = train_backwards
        self.chain_configs: dict = self.initialize_chain_configs(
            chain_configs, base_chain_config
        )

        # create slots for trained models and data
        self.is_fit: bool = False
        self.chains: dict = {}
        self.train_data: pd.DataFrame = pd.DataFrame()

    def initialize_chain_configs(
        self, chain_configs: dict, base_chain_config: dict
    ) -> dict:
        """initialize chain config dicts, filling in with base config and ensemble-wide `train_backwards`"""
        final_configs: dict = {c: base_chain_config for c in chain_configs}
        for col in final_configs:
            for key in chain_configs[col]:
                final_configs[col][key] = chain_configs[col][key]
            # this must agree across all chains
            final_configs[col]["train_backwards"] = self.train_backwards
        return final_configs

    def validate_training_args(self):
        """ensure indexes, columns, and chain config keys are all as expected"""
        assert all(k in self.train_data.columns for k in self.chain_configs.keys())
        assert self.train_data.index.names == [
            "concert_id",
            "selection_id",
        ], "Data must be indexed by concert_id and selection_id"
        assert "weight" in self.train_data.columns, "Data must contain a weight field"

    def train(self, data: pd.DataFrame, n_jobs: int = cpu_count()):
        """fit the chain models defined by chain_configs"""
        self.train_data = data
        self.validate_training_args()

        with Pool(n_jobs) as pool:
            job_data = [
                (self.train_data[col], kwargs)
                for col, kwargs in self.chain_configs.items()
            ]
            chains = pool.map(Chain.from_tuple, job_data, chunksize=1)

        self.chains = {c.name: c for c in chains}
        self.is_fit = True

        return self


class ChainEnsembleScorer:
    def __init__(
        self,
        model: ChainEnsemble,
        default_break_weight: int = 1,
        summary_function: str = "rpw",
    ):
        if not model.is_fit:
            raise ValueError(
                "ChainEnsemble model must be fit before being passed to ChainEnsembleScorer"
            )
        self.model = model
        self.default_break_weight = default_break_weight
        if summary_function not in AVAILABLE_SUMMARY_FUNCTIONS:
            raise ValueError(
                f'summary function unknown, please use one of ({", ".join(AVAILABLE_SUMMARY_FUNCTIONS)})'
            )
        self.summary_function = AVAILABLE_SUMMARY_FUNCTIONS[summary_function]

        # metadata on our scoring data frame
        self.break_idx: Optional[int] = None  # filled by collapse_training_data
        self.intermission_idx = None  # ''  ''
        self.raw_data: pd.DataFrame = self.collapse_training_data()

        # initialize state
        self.state: dict = {}
        self.score_data: pd.DataFrame = pd.DataFrame()
        self.is_clean_start: bool = False
        self.initialize_score_state()
        self.set_break_weight(self.default_break_weight)

    def collapse_training_data(self) -> pd.DataFrame:
        """Collapse the full training dataset down to the unique selections that will be scored,
        add a row representing the end of a program
        """
        data = (
            self.model.train_data.reset_index()
            .drop("concert_id", axis=1)
            .drop_duplicates("selection_id")
            .set_index("selection_id")
            .copy()
        )

        # handle the break record
        self.break_idx = data.index.max() + 1
        break_row = pd.Series({c: BREAK for c in data.columns}, name=self.break_idx)
        break_row["weight"] = 1
        data = data.append(break_row)

        # take note of the intermission idx
        self.intermission_idx = data.loc[data[data.columns[-1]] == INTERMISSION].index[
            0
        ]

        return data

    def reset_state(self):
        """initialize or reset state based on each config's state_size"""
        self.state = {
            k: (BREAK,) * self.model.chain_configs[k].get("state_size")
            for k in self.model.chain_configs
        }
        return self

    def initialize_score_state(self):
        """Set up a clean start of state and score_data"""
        self.reset_state()
        self.score_data = self.raw_data.copy()

        for c in self.model.chains:
            self.score_data[c] = self.model.chains[c].transform_scoring_series(
                self.score_data[c]
            )

        self.is_clean_start = True

        return self

    def set_break_weight(self, break_weight: int):
        self.score_data.loc[self.break_idx, "weight"] = break_weight
        self.score_data.loc[self.intermission_idx, "weight"] = break_weight
        return self

    def get_selection_features(self, selection_id: int) -> pd.Series:
        """return a series representing of a selection's feature values"""
        return self.score_data.loc[selection_id]

    def update_state(self, selection_id: int):
        """update state with the feature values of the most recent selection;
        accommodates chains of varying size
        """
        selection_features = self.get_selection_features(selection_id)
        for k in self.state:
            self.state[k] = self.state[k][1:] + (selection_features[k],)
        return self

    def scrub(self, selection_id: int = None):  # feature_values: dict = None):
        """cumulatively scrub rows from the scoring data frame by selection_id and/or by feature value"""
        if selection_id:
            if selection_id in self.score_data.index:
                self.score_data = self.score_data.drop(selection_id, axis=0)
        #
        # if feature_values:
        #     # TODO: transition to a cumulative index that's sliced only once
        #     for feature in feature_values:
        #         for val in feature:
        #             self.score_data = self.score_data.loc[self.score_data[feature] != val]

        return self

    def next_idx(
        self,
        feature_weights: dict,  # feature_limits: dict,
        weighted_average_exponent: float = 1.0,
        case_weight_exponent: float = 1.0,
        random_state: int = None,
    ) -> int:

        if self.is_clean_start:  # this method will dirty scorer state
            self.is_clean_start = False

        # accumulate score cols and weights as we score each feature column with the current state
        score_cols = []
        score_weights = np.array([], dtype=float)
        for col in feature_weights:
            if feature_weights[col] <= 0 or feature_weights[col] is None:
                continue
            score_col = col + "___score__"
            score_cols.append(score_col)
            score_weights = np.append(score_weights, feature_weights[col])
            self.score_data[score_col] = (
                self.model.chains[col]
                .score_series(self.score_data[col], self.state[col])
                .values
            )

        # filter to rows with no model scored as 0
        scorable_ids = self.score_data.index[
            (self.score_data[score_cols] == 0).sum(axis=1) == 0
        ]

        summarized_scores = self.summary_function(
            self.score_data.loc[scorable_ids, score_cols].values, score_weights
        )
        case_weights = self.score_data.loc[scorable_ids, "weight"]

        # apply non-linear transformations to the scores and case weights; normalize result to sum to 1
        final_scores = np.power(
            summarized_scores, weighted_average_exponent
        ) * np.power(case_weights, case_weight_exponent)
        final_scores = final_scores / final_scores.sum()

        # sample an index, seeding np's random number generator if needed; cast to int for sqlalchemy lookups
        np.random.seed(random_state)
        idx = int(np.random.choice(scorable_ids, p=final_scores))

        # update state, scrub the index from the score data, and return
        # selection_features = self.get_selection_features(idx)  # for scrubbing later
        self.update_state(idx)
        self.scrub(selection_id=idx)

        return idx

    def generate_program(
        self,
        feature_weights: dict,  # feature_limits: dict,
        weighted_average_exponent: float = 1.0,
        case_weight_exponent: float = 1.0,
        break_weight: int = None,
        random_state: int = None,
    ) -> list:

        # initialize if `next_idx` has been called since last initialized
        if not self.is_clean_start:
            self.initialize_score_state()

        # set break weight if it's not the default used in __init__
        if break_weight is not None and break_weight != self.default_break_weight:
            self.set_break_weight(break_weight)

        def local_next_idx():
            """helper to not pass the same options around everywhere"""
            return self.next_idx(
                feature_weights=feature_weights,
                weighted_average_exponent=weighted_average_exponent,
                case_weight_exponent=case_weight_exponent,
                random_state=random_state,
            )

        program: list = []

        selection_idx: int = local_next_idx()
        while selection_idx != self.break_idx:
            program.append(selection_idx)
            selection_idx = local_next_idx()

        if self.model.train_backwards:
            program = program[::-1]

        return program


if __name__ == "__main__":
    data_train = "adabaababbcbadcedfbcaeebcbcaacbcbabdbdbbac".split()
    s_train = pd.Series(data_train, index=[1] * len(data_train), name="training")
    x = Chain(s_train, cull_threshold=4)
    s_score = x.transform_scoring_series(
        pd.Series(["a", "b", "c", "a", "b", "a", "d", "d", "e", "f"])
    )

    in_value = ("d",)
    print(x.get_probas(in_value))
    print(x.minor_values)
    print(x.score_series(s_score, in_value))
