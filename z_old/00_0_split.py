# coding: utf-8

import json
import pandas as pd


def clean_concerts(concerts, programID):
    """Clean a program's concerts into a list of single-level dicts"""
    out = []
    concert_num = 1
    for c in concerts:
        # track programID and which number concert for the program
        c['programID'] = programID
        c['concert_num'] = concert_num
        out.append(c)
        concert_num += 1
    return out


def clean_works(works, programID):
    """ split apart work information, program-work information, pw-soloist,
        and pw-conductor information
    """
    # receptacles
    w = []
    pw = []
    pws = []
    pwc = []

    prog_order = 1  # initialize counter

    for work in works:
        # work metadata
        w_data = {k: work.get(k)
                  for k in ['ID', 'composerName', 'workTitle', 'movement']}
        w.append(w_data)

        # map programs to works, tracking program order
        pw_data = {
            'programID': programID, 'workID': work.get('ID'),
            'programOrder': prog_order
        }
        pw.append(pw_data)

        # track program-work-soloists
        for s in work.get('soloists'):
            pws_data = {'programID': programID, 'workID': work.get('ID')}
            pws_data.update(s)
            pws.append(pws_data)

        # track program-work-conductors
        pwc.append({
            'programID': programID, 'workID': work.get('ID'),
            'conductorName': work.get('conductorName')
        })

        prog_order += 1

    return {'w': w, 'pw': pw, 'pws': pws, 'pwc': pwc}


def process_programs(programs):
    """ process each program into its component parts. this is step one towards
        making this dataset more relationional
    """

    data = {  # receptacles for split data
        'prog': [],
        'prog_conc': [],
        'work': [],
        'prog_work': [],
        'prog_work_solo': [],
        'prog_work_cond': []
    }

    for p in programs:

        programID = p.get('programID')  # localize programID

        # programs and program-concerts
        data['prog'].append({
            k: p.get(k) for k in ['id', 'programID', 'season', 'orchestra']
        })
        data['prog_conc'] += clean_concerts(p.get('concerts'), programID)

        # parse work data, split into appropriate receptacles
        work_data = clean_works(p.get('works'), programID)

        data['work'] += work_data['w']
        data['prog_work'] += work_data['pw']
        data['prog_work_solo'] += work_data['pws']
        data['prog_work_cond'] += work_data['pwc']

    return data


def save_splitfiles(data):
    outdir = 'data/split/'
    for k in data.keys():
        outpath = outdir + str(k) + '_split.txt'
        d = pd.DataFrame(data[k])
        d.to_csv(outpath, sep='\t', encoding='utf8', index=False)
        print('Saved ' + outpath)


if __name__ == '__main__':
    # read data
    j = json.loads(open('data/raw/programs.json', 'r').read())
    programs = j['programs']
    data_dict = process_programs(programs)
    save_splitfiles(data_dict)
