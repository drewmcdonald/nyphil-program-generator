from nyp.raw.schema import Composer, MBZComposer
from .api import MBZAPI


# TODO: resolve cross-dependencies with nyp.raw.schema objects


class MBZComposerSearch(MBZAPI):
    """MBZ Composer Lookup Data
    NOTE: does not page through results. assumes if a good match isn't in the top 25 results, it doesn't exist"""

    def __init__(self, composer: Composer):
        super(MBZComposerSearch, self).__init__(endpoint='artist')
        self.composer: Composer = composer
        self.record_count: int = 0
        self.objects: [MBZComposer] = []
        self.best_match: MBZComposer = None
        self.retrieve()

    def __repr__(self):
        return f'<MBZComposerSearch for {self.composer.name}>'

    @property
    def request_params(self) -> dict:
        return {
            'query': 'name:{} AND type:person'.format(self.composer.name),
            'fmt': 'json'
        }

    def post_retrieve(self) -> None:
        """identify and count the records our search was interested in;
        convert them to MBZComposer objects"""
        if not self.is_retrieved:
            raise ValueError('No data yet retrieved, can\'t run post-retrieve')

        self.record_count = self.content.get('count')
        self.objects = [MBZComposer(self.composer, r) for r in self.content.get(self.endpoint + 's')]

    @classmethod
    def acceptable_match_filter(cls, match: MBZComposer) -> bool:
        """return true if both ratios are >= 70 and their average is >= 85"""
        return match.match_token_sort_ratio >= 70 and\
            match.match_partial_ratio >= 70 and\
            match.match_average_ratio >= 85

    def pick_best_match(self) -> MBZComposer:
        """pick the best match among mbz results; first by token comparison scores
        then by whoever has the most aliases (most famous)
        then by whoever has a composer tag
        then by whoever has 'composer' in their disambiguation
        """

        def exit_with_result(final_result):
            """helper to finalize the result once we find it"""
            if final_result:
                final_result.fill_additional_data()
                final_result.is_best_match = True
            self.best_match = final_result
            return final_result

        # filter to acceptable matches by token comparison scores
        acceptable_matches = list(filter(self.acceptable_match_filter, self.objects))

        # Take the match if there is only one; give up if there are none
        if len(acceptable_matches) == 1:
            return exit_with_result(acceptable_matches[0])
        if len(acceptable_matches) == 0:
            return exit_with_result(None)

        # list of all records' number of aliases
        all_n_aliases = [rec.n_aliases for rec in acceptable_matches if rec.n_aliases]
        # if any aliases at all, find the maximum number and filter to records with that number of aliases
        if len(all_n_aliases) > 0:
            max_n_aliases = max(all_n_aliases)
            most_aliases = list(filter(lambda rec: rec.n_aliases == max_n_aliases, acceptable_matches))
            # if only one record with the max number of aliases, that's our best match
            if len(most_aliases) == 1:
                return exit_with_result(most_aliases[0])

        # prefer the one result that's tagged as a composer if possible
        tagged_composer = list(filter(lambda rec: rec.tag_composer, acceptable_matches))
        if len(tagged_composer) == 1:
            return exit_with_result(tagged_composer[0])

        # prefer the one result that's disambiguated as a composer if possible
        disambiguated_composer = list(filter(lambda rec: rec.disambiguated_composer, acceptable_matches))
        if len(disambiguated_composer) == 1:
            return exit_with_result(disambiguated_composer[0])

        return exit_with_result(None)
