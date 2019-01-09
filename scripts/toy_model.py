import pandas as pd
from nyp.markov_chain import Chain
from random import choices

BREAK = '___BREAK__'
INTERMISSION = '-- Intermission --'

composers = pd.Series(pd.read_csv('testdata_composers.csv', index_col=0).name)

composers[composers == 'No Composer'] = INTERMISSION

composers_score = pd.Series(composers.unique().tolist() + [BREAK])

x = Chain(composers, state_size=2, cull=True, cull_threshold=5)

current_values = (BREAK, BREAK)

choice = ''
had_intermission = False

while choice != BREAK:

    scores = x.score_series(composers_score, current_values)
    choice = choices(population=scores.index, weights=scores.values, k=1)[0]

    # allow 'No Composer' (Intermission) only once
    if choice == INTERMISSION and had_intermission:
        while choice == INTERMISSION:
            choice = choices(population=scores.index, weights=scores.values, k=1)[0]
    elif choice == INTERMISSION:
        had_intermission = True

    if choice == BREAK:
        pass
    else:
        print(choice)

    current_values = (current_values[1], choice)
