from pandas import Series
import numpy as np


class ChainData(object):
    """
    build a Markov Chain from a categorical Series
    
    - data: a pandas Series
    - levels: MC depth (default 1)
    - cull: impute an 'other' value in place of values with very little usage

    """

    def __init__(self, data: Series, state_size: int=1, cull: bool=True):
        self.data = data.values
        self.name = data.name if data.name else 'unnamed'
        self.state_size = state_size
        self.cull = cull
        self.is_collapsed = False
        self.prob: dict = None

    def __repr__(self):
        return f'<Chain ({self.name}): {{{self.sample_data_str}}}>'

    @property
    def sample_data_str(self) -> str:
        """string of first few chain items"""
        if len(self.data) > 4:
            return ", ".join(self.data[:4].tolist() + ["..."])
        return ", ".join(self.data)

    def get_preprocessed(self):
        """
        pre-process the data (including culling) into a
        clean character vector
        """
        return

    def collapse_chain(self):
        """collapse the categorical vector """
        return


if __name__=='__main__':
    s = Series('a a b a b c'.split())
    x = ChainData(s, levels=1)

    print(x.collapse_chain())
