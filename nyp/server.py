from flask import Flask, jsonify, request

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from nyp.markov import ChainEnsemble
from nyp.models import Selection

from typing import List

import pickle

model: ChainEnsemble = pickle.load(open('/Users/drew/Desktop/nyp/data/model_v1.p', 'rb'))
Session = sessionmaker(create_engine('sqlite:////Users/drew/Desktop/nyp/data/raw.db'))


app = Flask(__name__)

prediction_weights = {
    'work_type': 2.0,
    'composer_country': 1.0,
    'composer_birth_century': 1.0,
    'soloist_type': 2.0,
    'percent_after_intermission_bin': 6.0
}

# this is pretty good!
# ?percent_after_intermission_bin=100&work_type=50&soloist_type=25&composer_birth_century=25&composer_country=25


@app.route('/generate', methods=['GET'])
def generate_program():
    session = Session()
    q = session.query(Selection)

    # parse request (just weights from command args for now)
    weights = {k: int(v) for k, v in request.args.items()} or prediction_weights

    program = model.generate_program(weights,
                                     break_weight=60,
                                     case_weight_exponent=.8)

    final_program: List[Selection] = []  # receptacle

    for p in program:
        selection = q.get(int(p))
        final_program.append(selection.to_dict())

    return jsonify(final_program)


if __name__ == '__main__':
    app.run()
