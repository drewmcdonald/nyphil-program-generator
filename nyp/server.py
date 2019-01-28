from flask import Flask, jsonify, request

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from nyp.markov import ChainEnsemble
from nyp.models import Selection

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


@app.route('/generate', methods=['GET'])
def generate_program():
    session = Session()
    program = model.generate_program(prediction_weights, break_weight=60)
    q = session.query(Selection)
    final_program = []
    for p in program:
        final_program.append(str(q.get(int(p))))
    session.close()
    return jsonify(final_program)


if __name__ == '__main__':
    app.run()
