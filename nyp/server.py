from flask import Flask, jsonify, request

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from nyp.markov import ChainEnsemble, ChainEnsembleScorer
from nyp.models import Selection

import pickle
from copy import deepcopy

from dotenv import load_dotenv
from os import getenv

load_dotenv()

Session = scoped_session(sessionmaker(create_engine(getenv('MYSQL_CON'))))

app = Flask(__name__)

# shared objects

model: ChainEnsemble = pickle.load(open('data/model_v1.p', 'rb'))
scorer_template: ChainEnsembleScorer = ChainEnsembleScorer(model)

default_prediction_weights = {
    'work_type': 4.0,
    'composer_country': 1.0,
    'composer_birth_century': 1.0,
    'soloist_type': 2.0,
    'percent_after_intermission_bin': 4.0
}


def make_scorer() -> ChainEnsembleScorer:
    """get a copy of the clean, initialized scorer_template to avoid redundant computation
    use a shallow copy so the model object tied to the scorer is not passed around unnecessarily"""
    return deepcopy(scorer_template)


@app.route('/generate', methods=['GET'])
def generate_program():
    scorer = make_scorer()
    program = scorer.generate_program(default_prediction_weights,
                                      break_weight=1,
                                      weighted_average_exponent=1.5,
                                      case_weight_exponent=.25)

    s = Session
    q = s.query(Selection)

    final_program = [q.get(s).to_dict() for s in program]

    s.remove()
    del scorer

    return jsonify(final_program)


if __name__ == '__main__':
    app.run()
