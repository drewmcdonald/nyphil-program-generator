import json
import requests
from time import sleep

import pandas as pd


BASE_URL = 'https://musicbrainz.org/ws/2/'
APP_HEADERS = {'User-Agent': 'NYPhil Concert Builder/0.01 (https://github.com/drewmcdonald/nyphil_concert_builder)'}


def search_composer(id, name):
    endpoint = 'artist'
    params = {
        'query': '{} AND type:person'.format(name),
        'fmt': 'json',
        'inc': 'aliases url-rels'
    }
    url = BASE_URL + endpoint
    r = requests.get(url, params=params, headers=APP_HEADERS)
    if r.status_code == 200:
        data = json.loads(r.content)
        data['search'] = {'id': id, 'name': name}
        return data
    if r.status_code == 503:
        print('Failed on {} ({})!'.format(id, name))

    print("Status code {} for {} ({})".format(r.status_code, id, name))


def main():

    comp = pd.read_csv('data/interim/composer_key.txt', sep='\t')
    has_bracket = comp.composerName.str.contains('\[')
    comp.loc[has_bracket, 'composerName'] = \
        comp.loc[has_bracket].composerName.str.replace(r' \[ ?[^ ,]* ?\]', '')

    results = []

    for i, c in comp.loc[comp.composerName != 'No Composer'].iterrows():
        if i % 100 == 0:
            print(i)
        try:
            this_result = search_composer(c.composerID, c.composerName)
        except Exception as e:
            with open('composer_match_temp.json', 'w') as f:
                json.dump(results, f)
            print('Failed on ' + str(c.composerID))
            print('Wrote progress to composer_match_temp.json')
            raise(e)
        results.append(this_result)
        sleep(1)

    with open('data/interim/mbz_composer_search.json', 'w') as f:
        json.dump(results, f)
        print('Wrote results to data/interim/mbz_composer_search.json')


if __name__ == '__main__':
    main()
