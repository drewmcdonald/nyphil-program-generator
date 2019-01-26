import pandas as pd
from collections import defaultdict
from typing import Union

BREAK = "___BREAK__"
MINOR = "___MINOR__"


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
        probas = {k: v / counts[k] for k, v in probas.items()}

        # use our probas as a lookup, else 0 if that doesn't fill it
        new_data = new_data.map(probas)
        new_data.fillna(0, inplace=True)

        return new_data


if __name__ == '__main__':
    data_train = 'a d a b a a b a b b c b a d c e d f b c a e e b c b c a a c b c b a b d b d b b a c'.split()
    s_train = pd.Series(data_train, index=[1]*len(data_train), name='training')
    x = Chain(s_train, cull_threshold=4)
    s_score = x.transform_scoring_series(pd.Series(['a', 'b', 'c', 'a', 'b', 'a', 'd', 'd', 'e', 'f']))

    in_value = ('d', )
    print(x.get_probas(in_value))
    print(x.minor_values)
    print(x.score_series(s_score, in_value))

