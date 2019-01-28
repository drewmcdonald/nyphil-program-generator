import pandas as pd
from collections import defaultdict
from multiprocessing import Pool, cpu_count
from typing import Union

BREAK = "___BREAK__"
MINOR = "___MINOR__"
INTERMISSION = "___INTERMISSION__"
BREAK_IDX = 999999


def _internal_defaultdict_int():
    """module level function to make our defaultdict code pickle-able"""
    return defaultdict(int)


class Chain(object):
    """
    build a Markov Chain from a categorical Series
    
    - data: a pandas Series with an index that defines the unit of analysis (usually a concert)
    - state_size: MC depth (default 1)
    - train_backwards: fit chain in reverse program order (recommended)
    - cull: impute an 'other' value in place of values with very little usage (default True)
    - cull_threshold: the floor below which a value is replaced with OTHER; interpreted as a percentage of total records
         if between 0 and .999; otherwise interpreted as a count of appearances if greater than or equal to 1
         (default .01)
    """

    def __init__(self, data: pd.Series, state_size: int = 1, train_backwards: bool = True,
                 cull: bool = True, cull_threshold: Union[int, float] = 0.01):
        self.name: str = data.name or 'unnamed'
        self.state_size: int = state_size
        self.train_backwards: bool = train_backwards
        self.cull: bool = cull
        self.cull_threshold: Union[int, float] = cull_threshold

        self.minor_values: list = []

        new_data = data.copy()  # don't modify in place
        self.data: pd.Series = self.pre_process_data(new_data)
        self.counts: dict = self.fill_counts()
        self.probas: dict = {k: self.counts_to_probabilities(v)
                             for k, v in self.counts.items()}

    def __repr__(self):
        return f'<Chain ({self.name}): {{{self.sample_data_str}}}>'

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
        values = break_values.copy()

        for i, d in data.groupby(level=0):
            values += d.values.tolist() + break_values

        values = pd.Series(values)

        if self.cull:

            if self.cull_threshold < 0:
                raise ValueError('cull_threshold should be >= 0')

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
        loosely adapted from jsvine/markovify
        major difference is that this is set to accept a full corpus all at once with beginning/end markers interpolated
        """
        lookup = defaultdict(_internal_defaultdict_int)

        # accumulate counts of each input to output
        for i in range(len(self.data) - self.state_size):
            in_val = tuple(self.data.iloc[i:(i + self.state_size)])
            out_val = self.data.iloc[i + self.state_size]

            lookup[in_val][out_val] += 1

        return lookup

    def get_probas(self, in_val: tuple):
        """fetch the probabilities for a given input"""

        if len(in_val) != self.state_size:
            raise ValueError('Input value length does not equal Chain state size')

        # replace minor values with placeholder
        in_val = tuple(i if i not in self.minor_values else MINOR for i in in_val)

        # this has to be explicit since self.probas is now a default dict
        if in_val not in self.probas:
            raise ValueError(f'Value {in_val} is not keyed in chain data for {self.name} chain')

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


class ChainEnsembleScoreData(object):

    def __init__(self, training_data: pd.DataFrame, break_weight: int = 100):
        """
        create and manage the scoring set used in a ChainEnsemble

        :param training_data: the full training data frame from a parent ChainEnsemble
        :param break_weight: weight at which to set BREAK (compared to other selections' actual performance instances)
        """
        self.break_weight: int = break_weight
        self.break_idx: int = None
        self.data: pd.DataFrame = self.collapse_training_data(training_data)
        self.intermission_idx = self.find_intermission_idx()
        self.score_cols: list = None  # hold the column names for scores to be used in the current generation
        self.total_weight: float = None  # hold the current total weight of all scoring columns
        self.final_scores: pd.Series = None

    def collapse_training_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Collapse a full training dataset down to the unique selections that will be scored,
        add a row representing the end of a program
        """
        data = data.reset_index() \
            .drop('concert_id', axis=1) \
            .drop_duplicates('selection_id') \
            .set_index('selection_id') \
            .copy()
        self.break_idx = data.index.max() + 1
        break_row = pd.Series({c: BREAK for c in data.columns}, name=self.break_idx)
        break_row['weight'] = self.break_weight
        data = data.append(break_row)
        return data

    def find_intermission_idx(self) -> int:
        """locate the row index representing intermission"""
        return self.data.loc[self.data[self.data.columns[-1]] == INTERMISSION].index[0]

    def apply_chain_transformations(self, chains: dict):
        """apply each chain's pre-processing transformation to it's column on the score data frame"""
        for c in chains:
            self.data[c] = chains[c].transform_scoring_series(self.data[c])
        return self

    def scrub(self, selection_id: int = None, feature_values: dict = None):
        """cumulatively scrub rows from the scoring data frame by selection_id and/or by feature value

        :param selection_id: an index integer to remove from the scoring set
        :param feature_values: a dictionary keyed by column name with lists of values that should be
            removed from scoring
        :return: self
        """
        if selection_id:
            if selection_id in self.data.index:
                self.data = self.data.drop(selection_id, axis=0)

        if feature_values:
            # TODO: transition to a cumulative index that's sliced only once
            for feature in feature_values:
                for val in feature:
                    self.data = self.data.loc[self.data[feature] != val]

        return self

    def get_selection_features(self, selection_id: int) -> pd.Series:
        """return a series representing of a selection's feature values"""
        return self.data.loc[selection_id]

    def fill_score_columns(self, state: dict, chains: dict, weights: dict):

        self.score_cols = []  # wipe in case weights have changed
        self.total_weight = 0
        for col in weights:
            if weights[col] <= 0:
                continue
            score_col = col + '___score__'
            self.score_cols.append(score_col)
            self.total_weight += weights[col]
            self.data[score_col] = chains[col].score_series(self.data[col], state[col]).values * weights[col]

        return self

    def aggregate_final_score(self, case_weight_exponent: float = 1.0):
        selection_usable_idx = (self.data[self.score_cols] == 0).sum(axis=1) == 0
        self.final_scores = (
                self.data.loc[selection_usable_idx, self.score_cols].sum(axis=1)
                / self.total_weight
                * self.data.loc[selection_usable_idx, 'weight'].pow(case_weight_exponent)
        )
        return self

    def sample(self) -> int:
        return self.final_scores.sample(weights=self.final_scores).index[0]


class ChainEnsemble(object):

    def __init__(self, chain_configs: dict, base_chain_config: dict, train_backwards: bool = True):
        self.train_backwards = train_backwards
        self.chain_configs: dict = self.initialize_chain_configs(chain_configs, base_chain_config)

        # initialize state
        self.state: dict = {}
        self.reset_state()

        # create slots for trained models and data
        self.chains: dict = None
        self.train_data: pd.DataFrame = None
        self.score_data: ChainEnsembleScoreData = None

    def initialize_chain_configs(self, chain_configs: dict, base_chain_config: dict) -> dict:
        """initialize chain config dicts, filling in with base config and overwriting ensemble-wide train_backwards"""
        final_configs: dict = {c: base_chain_config for c in chain_configs}
        for col in final_configs:
            for key in chain_configs[col]:
                final_configs[col][key] = chain_configs[col][key]
            # this must agree across all chains
            final_configs[col]['train_backwards'] = self.train_backwards
        return final_configs

    def reset_state(self):
        """initialize or reset state based on each config's state_size"""
        self.state = {k: (BREAK,) * self.chain_configs[k].get('state_size') for k in self.chain_configs}
        return self

    def update_state(self, selection_id: int):
        """update state with the feature values of the most recent selection"""
        selection_features = self.score_data.get_selection_features(selection_id)
        for k in self.state:
            # the 1: slice allows for chains of varying state_sizes
            self.state[k] = self.state[k][1:] + (selection_features[k],)
        return self

    def validate_training_args(self):
        """ensure indexes, columns, and chain config keys are all as expected"""
        assert all(k in self.train_data.columns for k in self.chain_configs.keys())
        assert self.train_data.index.names == ['concert_id', 'selection_id'], \
            "Data must be indexed by concert_id and selection_id"
        assert 'weight' in self.train_data.columns, \
            "Data must contain a weight field"

    def train(self, data: pd.DataFrame, n_jobs: int = cpu_count()):
        """fit the chain models defined by chain_configs; collapse score_data and transform per the chains"""
        self.train_data = data
        self.validate_training_args()

        with Pool(n_jobs) as pool:
            # list of tuples of (series, kwargs)
            job_data = [(self.train_data[col], kwargs) for col, kwargs in self.chain_configs.items()]
            chains = pool.map(Chain.from_tuple, job_data, chunksize=1)

        self.chains = {c.name: c for c in chains}

        return self

    def generate_program(self, feature_weights: dict,  # feature_limits: dict,
                         break_weight: int = 100,
                         case_weight_exponent: float = 1.0):

        self.reset_state()
        self.score_data = ChainEnsembleScoreData(self.train_data, break_weight=break_weight)
        self.score_data.apply_chain_transformations(self.chains)

        # self.score_data.fill_score_columns(self.state, self.chains, feature_weights)

        def next_idx() -> int:
            return self.score_data \
                .fill_score_columns(self.state, self.chains, feature_weights) \
                .aggregate_final_score(case_weight_exponent) \
                .sample()

        program: list = []

        selection_idx: int = next_idx()
        while selection_idx != self.score_data.break_idx:
            # selection_features = self.score_data.get_selection_features(selection_idx)
            self.update_state(selection_idx)
            self.score_data.scrub(selection_id=selection_idx)
            program.append(selection_idx)
            selection_idx = next_idx()

        if self.train_backwards:
            program = program[::-1]

        return program


if __name__ == '__main__':
    data_train = 'a d a b a a b a b b c b a d c e d f b c a e e b c b c a a c b c b a b d b d b b a c'.split()
    s_train = pd.Series(data_train, index=[1] * len(data_train), name='training')
    x = Chain(s_train, cull_threshold=4)
    s_score = x.transform_scoring_series(pd.Series(['a', 'b', 'c', 'a', 'b', 'a', 'd', 'd', 'e', 'f']))

    in_value = ('d',)
    print(x.get_probas(in_value))
    print(x.minor_values)
    print(x.score_series(s_score, in_value))
