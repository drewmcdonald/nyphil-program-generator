from pandas import Series
import numpy as np


BEGIN = "___BEGIN__"
END = "___END__"


def counts_to_probability(counts: dict):
    total = sum(counts.values())
    probs = {}
    for c in counts:
        probs[c] = counts[c] / total
    return probs


class ChainData(object):
    """
    build a Markov Chain from a categorical Series
    
    - data: a pandas Series
    - state_size: MC depth (default 1)
    - cull: impute an 'other' value in place of values with very little usage

    """

    def __init__(self, data: Series, state_size: int=1, cull: bool=True):
        self.data: Series = data.values
        self.name: str = data.name if data.name else 'unnamed'
        self.state_size: int = state_size
        self.cull: bool = cull
        self.is_collapsed: bool = False
        self.prob: dict = self.collapse_chain()

    def __repr__(self):
        return f'<Chain ({self.name}): {{{self.sample_data_str}}}>'

    @property
    def sample_data_str(self) -> str:
        """string of first few chain items"""
        if len(self.data) > 4:
            return ", ".join(self.data[:4].tolist() + ["..."])
        return ", ".join(self.data)

    def preprocess_data(self):
        """pre-process the data (including culling) into a clean character vector"""
        return

    def collapse_chain(self) -> dict:
        """collapse the categorical vector into a dict of probability lookups
        loosely adapted from jsvine/markovify
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

            lookup[in_val][out_val] += 1

        # convert to probabilities
        lookup = {k: counts_to_probability(v) for k, v in lookup.items()}

        self.is_collapsed = True

        return lookup


if __name__=='__main__':
    s = Series('a a b a a b a b b c b a b c a b c b c a a c b c b a b b b b a c'.split())
    x = ChainData(s, state_size=2)

    print(x.prob)
