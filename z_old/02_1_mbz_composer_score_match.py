import json
import pandas as pd
import unicodedata
from fuzzywuzzy import fuzz


ARTIST_NULLKEYS = [
    'mbzid', 'country', 'gender', 'sortname', 
    'area_name', 'beginarea_name', 'endarea_name', 'beginarea_id', 'endarea_id',
    'lifespan_begin', 'lifespan_end', 'lifespan_ended'
]


def normalize(string):
    return str(unicodedata.normalize('NFKD', string).encode('ASCII', 'ignore'))


def flatten_artist(artist):
    final = {k: None for k in ARTIST_NULLKEYS}
    final['n_aliases'] = 0
    final['n_tags'] = 0
    final['disambig_composer'] = False
    final['tag_composer'] = False

    # basics
    final['mbzid'] = artist['id']
    final['country'] = artist.get('country')
    final['gender'] = artist.get('gender')
    final['sortname'] = artist.get('sort-name')

    if artist.get('area'):
        final['area_name'] = artist.get('area').get('name')

    if artist.get('begin-area'):
        final['beginarea_name'] = artist.get('begin-area').get('name')
        final['beginarea_id'] = artist.get('begin-area').get('id')
    
    if artist.get('end-area'):
        final['endarea_name'] = artist.get('end-area').get('name')
        final['endarea_id'] = artist.get('end-area').get('id')

    ls = artist.get('life-span')
    if ls:
        final['lifespan_begin'] = ls.get('begin')
        final['lifespan_end'] = ls.get('end')
        final['lifespan_ended'] = ls.get('ended')

    if artist.get('aliases'):
        final['n_aliases'] = len(artist['aliases'])

    tags = artist.get('tags')
    if tags:
        final['n_tags'] = len(tags)
        final['tag_composer'] = any([t['name'] == 'composer' for t in tags])

    disambig = artist.get('disambiguation')
    if disambig:
        final['disambig_composer'] = 'composer' in disambig

    return final


def first_pass(data):
    final = []
    for i, d in enumerate(data):
        # skip if nothing found by mbz
        if d is None:
            continue

        search = d.get('search')
        clean_name = search.get('name').replace('  ', ' ').replace(',', '').lower()
        clean_name = normalize(clean_name.replace(' sir ', ' '))
        artists = d.get('artists')

        for a in artists:
            
            # cut based off of mbz search 'score'
            if int(a['score']) < 95:
                continue
            
            # cut based off of fuzzy match score of names
            found_name = normalize(a['sort-name'].lower().replace(', ', ' '))
            tsr_score = fuzz.token_sort_ratio(clean_name, found_name)
            pr_score = fuzz.partial_ratio(clean_name, found_name)
            avg_score = (tsr_score + pr_score) / 2
            
            if avg_score < 80 or tsr_score < 70 or pr_score < 70:
                continue

            # save
            this_meta = {'index': i, 
                         'searchname': search.get('name'), 
                         'composerid': search.get('id'), 
                         'found_name': found_name, 'clean_name': clean_name,
                         'tsr_score': tsr_score, 'pr_score': pr_score, 
                         'avg_score': avg_score}
            this_match = flatten_artist(a)
            this_match.update(this_meta)

            final += [this_match]

    return pd.DataFrame(final)


def main():
    # load data
    with open('data/interim/mbz_composer_search.json', 'r') as f:
        data = json.load(f)


    df = first_pass(data)


# if one result w/ average score >= 80 and both scores >= 70, accept as a match

# if more than one result with an avg. score >= 80 and both >=70, 
# take the one with the most aliases (most famous) OR one with tag 'composer' OR disambiguation like 'composer'

# else no match
