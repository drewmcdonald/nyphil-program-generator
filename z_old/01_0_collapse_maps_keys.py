import pandas as pd


def collapse_conductor():
    """create table of unique conductors, swap prog-work marker with ID"""

    # read in prog-work-conductor table, fix missing
    df = pd.read_table('data/split/prog_work_cond_split.txt', sep='\t')
    df.loc[pd.isnull(df.conductorName), 'conductorName'] = 'Not conducted'

    # make a new df holding unique name and a new id
    names = pd.DataFrame({'conductorName': df.conductorName.unique()})
    names['conductorID'] = names.index + 1

    # export the mapping table, minus the repeated name field
    df = df.merge(names, on='conductorName').drop('conductorName', 1)
    df.to_csv('data/interim/prog_work_cond_map.txt', sep='\t', index=False)
    print('Saved data/interim/prog_work_cond_map.txt')

    # export the conductor name lookup table
    names.to_csv('data/interim/conductor_key.txt', sep='\t', index=False)
    print('Saved data/interim/conductor_key.txt')


def collapse_composer_work():
    """split the work_split table into works and composers"""

    # read data, drop dupes
    works = pd.read_table('data/split/work_split.txt', sep='\t')
    works.drop_duplicates(inplace=True)

    # split workID into master work and movement IDs
    IDsplit = works.ID.str.split(pat=r'\*', expand=True)
    IDsplit.rename(columns={0: 'masterID', 1: 'movementID'}, inplace=True)
    # fill missing movement IDs with 0
    IDsplit = IDsplit.apply(axis=0,
                            func=lambda x: x.replace({'': 0}).astype(int))
    # merge work IDs back into works table
    works = works.merge(IDsplit, left_index=True, right_index=True)
    works['movement'] = works.movement.fillna('')

    # split off composer name into a key table, merge back to work
    composers = pd.Series(works.composerName.unique())
    composers = pd.DataFrame(
        {'composerID': composers.index, 'composerName': composers}
    )
    composers.loc[pd.isnull(composers.composerName), 'composerName'] = \
        'No Composer'
    # edge case due to slash
    composers.loc[composers.composerName == 'Norpoth,  John-Paul/Jannina',
                  'composerName'] = 'Norpoth,  John-Paul'
    composers.to_csv('data/interim/composer_key.txt', sep='\t', index=False)
    print('Saved data/interim/composer_key.txt')

    works = pd.merge(works, composers, on='composerName')\
        .drop('composerName', axis=1)
    works.to_csv('data/interim/work_key.txt', sep='\t', index=False)
    print('Saved data/interim/work_key.txt')


def collapse_work_solos():
    """collapse soloist table into flags of work solos by category"""

    # read in prog-work-solo table and manual key of instrument types
    df = pd.read_table('data/split/prog_work_solo_split.txt', sep='\t')
    inst = pd.read_table('data/manual/solo_instrument_type.txt', sep='\t')

    # relabel instrumentType to eventually be a bettter column name
    inst.soloistType = 'has_' + inst.soloistType.str.lower() + '_solo'

    # filter to solos only (no accompaniment)
    df = df.loc[df.soloistRoles == 'S']
    df.drop(['programID', 'soloistName', 'soloistRoles'], 1, inplace=True)

    # left merge in instrument categories, fill missing as 'Other'
    df = pd.merge(df, inst, on='soloistInstrument', how='left')
    df['soloistType'] = df.soloistType.fillna('has_other_solo')
    df.drop('soloistInstrument', 1, inplace=True)
    df['counter'] = 1  # counter for pivoting

    # pivot by work, flagging instrument types
    work_solos = df.pivot_table(
        index='workID', columns='soloistType', values='counter',
        aggfunc=lambda x: int(sum(x) > 0), fill_value=0
    )

    # write
    work_solos.to_csv('data/interim/work_solos.txt', sep='\t',
                      index=True, index_label='workID')
    print('Saved data/interim/work_solos.txt')


if __name__ == '__main__':
    print('Creating Key and Mapping Tables')
    collapse_conductor()
    collapse_composer_work()
    collapse_work_solos()
