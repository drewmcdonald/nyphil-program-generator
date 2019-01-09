from pandas import Series
from typing import Union

BREAK = "___BREAK__"
MINOR = "___MINOR__"


class Chain(object):
    """
    build a Markov Chain from a categorical Series
    
    - data: a pandas Series with an index that defines the unit of analysis (usually a concert)
    - state_size: MC depth (default 1)
    - cull: impute an 'other' value in place of values with very little usage (default True)
    - cull_threshold: the floor below which a value is replaced with OTHER; interpreted as a percentage of total records
         if between 0 and .999; otherwise interpreted as a count of appearances if greater than or equal to 1
         (default .01)
    """

    def __init__(self, data: Series, state_size: int=1, cull: bool=True, cull_threshold: Union[int, float]=0.01):
        self.name: str = data.name or 'unnamed'
        self.state_size: int = state_size
        self.cull: bool = cull
        self.cull_threshold: Union[int, float] = cull_threshold

        self.minor_values: list = []

        new_data = data.copy()  # don't modify in place
        self.data: Series = self.pre_process_data(new_data)
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

    def pre_process_data(self, data: Series) -> Series:
        """pre-process the data (including culling) into a clean character vector"""

        # initialize a receptacle and break values
        break_values = [BREAK] * self.state_size
        values = break_values.copy()

        for i, d in data.groupby(level=0):
            values += d.values.tolist() + break_values

        values = Series(values)

        if self.cull:

            if self.cull_threshold < 0:
                raise ValueError('cull_threshold should be >= 0')

            summary = values[values != BREAK].value_counts()

            # convert summary to percentages if needed
            if 0 < self.cull_threshold < 1:
                summary = summary / sum(summary)

            self.minor_values = summary.index[summary < self.cull_threshold].values

            values[values.isin(self.minor_values)] = MINOR

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
        lookup = {}

        # accumulate counts of each input to output
        for i in range(len(self.data) - self.state_size):

            in_val = tuple(self.data[i:(i + self.state_size)])
            out_val = self.data[i + self.state_size]

            if in_val not in lookup:
                lookup[in_val] = {}

            if out_val not in lookup[in_val]:
                lookup[in_val][out_val] = 0

            # increment
            lookup[in_val][out_val] += 1

        return lookup

    def get_probas(self, in_val: tuple):
        """fetch the probabilities for a given input"""

        if len(in_val) != self.state_size:
            raise ValueError('Input value length does not equal Chain state size')

        # replace minor values with placeholder
        in_val = tuple(i if i not in self.minor_values else MINOR for i in in_val)

        try:
            result = self.probas[in_val]
        except KeyError:
            raise ValueError(f'Value {in_val} is not keyed in chain data for {self.name} chain')

        return result

    def score_series(self, new_data: Series, in_val: tuple) -> Series:
        """apply the modeled scores to a new series of data"""

        probas = self.get_probas(in_val)
        new_data = Series(new_data.values, index=new_data.values, name=new_data.name)

        # use our probas as a lookup
        new_data = new_data.map(probas)

        # propagate the minor score to the actual minor values
        if MINOR in probas:
            new_data[new_data.index.isin(self.minor_values)] = probas[MINOR] / len(self.minor_values)

        # probability is 0 if still unfilled
        new_data.fillna(0, inplace=True)

        return new_data


if __name__ == '__main__':
    data_train = 'a d a b a a b a b b c b a d e d f b c a e e b c b c a a c b c b a b d b d b b a c'.split()
    data_score = ['a', 'b', 'c', 'd', 'e', 'f']
    s_train = Series(data_train, index=[1]*len(data_train), name='training')
    s_score = Series(data_score, index=data_score, name='scoring')
    x = Chain(s_train)

    in_value = ('d', )
    print(x.get_probas(in_value))
    print(x.score_series(s_score, in_value))

    import pandas as pd
    composers = pd.Series(pd.read_csv('../testdata_composers.csv', index_col=0).name)
    composers_score = pd.Series(composers.unique().tolist() + [BREAK])
    x = Chain(composers, state_size=2, cull=True, cull_threshold=4)
    in_value = ('Beethoven, Ludwig van', 'Mozart, Wolfgang Amadeus')
    print(x.get_probas(in_value))
    print(x.score_series(composers_score, in_value).sort_values(ascending=False).head(20))

    x = Chain(composers, state_size=1, cull=True, cull_threshold=4)
    in_value = ('Beethoven, Ludwig van',)
    print(x.get_probas(in_value))
    print(x.score_series(composers_score, in_value).sort_values(ascending=False).head(20))




