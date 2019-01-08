from pandas import Series
from typing import Union

OTHER = "___OTHER__"


class Chain(object):
    """
    build a Markov Chain from a categorical Series
    
    - data: a pandas Series
    - state_size: MC depth (default 1)
    - cull: impute an 'other' value in place of values with very little usage
    - cull_threshold: the floor below which a value is replaced with OTHER; interpreted as a percentage of total records
         if between 0 and .999; otherwise interpreted as a count of appearances if greater than or equal to 1
    """

    def __init__(self, data: Series, state_size: int=1, cull: bool=True, cull_threshold: Union[int, float]=0.01):
        self.raw_data: Series = data
        self.name: str = data.name or 'unnamed'
        self.state_size: int = state_size
        self.cull: bool = cull
        self.cull_threshold: Union[int, float] = cull_threshold

        self.data: Series = self.pre_process_data(data)
        self.counts: dict = self.fill_counts()
        self.probas: dict = {k: self.counts_to_probabilities(v) for k, v in self.counts.items()}

    def __repr__(self):
        return f'<Chain ({self.name}): {{{self.sample_data_str}}}>'

    @property
    def sample_data_str(self) -> str:
        """string of first few chain items"""
        if len(self.data) > 4:
            return ", ".join(self.data[:4].tolist() + ["..."])
        return ", ".join(self.data)

    @classmethod
    def counts_to_probabilities(cls, counts: dict):
        total = sum(counts.values())
        return {c: counts[c] / total for c in counts}

    def pre_process_data(self, data: Series):
        """pre-process the data (including culling) into a clean character vector"""
        if self.cull:

            if self.cull_threshold < 0:
                raise ValueError('cull_threshold should be >= 0')

            summary = data.value_counts()

            # convert summary to percentages if needed
            if 0 < self.cull_threshold < 1:
                summary = summary / sum(summary)

            keep = summary.index[summary >= self.cull_threshold]

            data[~data.isin(keep)] = OTHER

        return data

    def fill_counts(self) -> dict:
        """collapse the categorical vector into a dict of probability lookups
        loosely adapted from jsvine/markovify
        major difference is that this is set to accept a full corpus all at once with beginning/end markers imputed
        """

        lookup = {}

        # accumulate counts of each input to output
        for i in range(len(self.data)-self.state_size):

            in_val = tuple(self.data[i:(i+self.state_size)])
            out_val = self.data[i + self.state_size]

            if in_val not in lookup:
                lookup[in_val] = {}

            if out_val not in lookup[in_val]:
                lookup[in_val][out_val] = 0

            # increment
            lookup[in_val][out_val] += 1

        return lookup


if __name__=='__main__':
    s = Series('a a b a a b a b b c b a e f b c a e e b c b c a a c b c b a b b b b a c'.split())
    x = Chain(s, state_size=3, cull_threshold=5)

    print(x.probas)
    print(x.counts)
    print(x.data)
