from .api import MBZLookup


class MBZAreaLookup(MBZLookup):
    """Object to represent an MBZ Area
    recursively calls parent relationships to find the area's country
    """

    def __init__(self, mbz_id: str):
        super(MBZAreaLookup, self).__init__(endpoint='area', mbz_id=mbz_id)
        self.name = None
        self.sort_name = None
        self.iso_2_code: str = None
        self.iso_1_code: str = None
        self.retrieve()
        self.recurse_parents()

    def __repr__(self):
        return f'<MBZAreaLookup for {self.name}>'

    @property
    def request_params(self) -> dict:
        return {'fmt': 'json', 'inc': 'area-rels'}

    def post_retrieve(self):
        """Fill object attributes from the json response"""

        if not self.is_retrieved:
            raise ValueError('No data yet retrieved')

        self.name = self.content.get('name')
        self.sort_name = self.content.get('sort-name')

        iso_1_codes = self.content.get('iso-3166-1-codes')
        iso_2_codes = self.content.get('iso-3166-2-codes')

        if iso_1_codes:
            self.iso_1_code = iso_1_codes[0]
        if iso_2_codes:
            self.iso_2_code = iso_2_codes[0]

    def recurse_parents(self):
        """go up the chain of parent areas until we can fill out
        regional and country codes
        """
        # stop if we already have a country code
        if self.iso_1_code:
            return

        # filter to up-hierarchy relationships
        parent_rels = [x for x in self.content['relations']
                       if x['direction'] == 'backward']
        # share any found iso codes
        for parent_rel in parent_rels:
            parent_obj = MBZAreaLookup(parent_rel['area']['id'])

            self.iso_2_code = parent_obj.iso_2_code
            self.iso_1_code = parent_obj.iso_1_code
