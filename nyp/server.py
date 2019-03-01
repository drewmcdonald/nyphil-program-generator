from flask import Flask, jsonify, request
from flask_cors import cross_origin
from nyp.markov import ChainEnsemble, ChainEnsembleScorer
import random

# database imports
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from nyp.models import Selection

# handle persistent model objects
import pickle
from copy import deepcopy

# environment
from dotenv import load_dotenv
from os import getenv

load_dotenv()

application = Flask(__name__)
application.secret_key = getenv('APP_SECRET')

if getenv('RDS_HOSTNAME'):
    connection_string = "mysql+pymysql://{}:{}@{}:{}/nyphil".format(
        getenv('RDS_USERNAME'), getenv('RDS_PASSWORD'), getenv('RDS_HOSTNAME'), getenv('RDS_PORT')
    )
else:
    connection_string = getenv('MYSQL_CON_DEV')

Session = scoped_session(sessionmaker(create_engine(connection_string)))


# shared objects
model: ChainEnsemble = pickle.load(open('data/model_v1.p', 'rb'))
scorer_template: ChainEnsembleScorer = ChainEnsembleScorer(model)

# base
DEFAULTS = {
    'random_state': None,
    'break_weight': 1,
    'weighted_average_exponent': 1.2,
    'case_weight_exponent': .25,
    'feature_weights': {
        'work_type': 4.0,
        'composer_country': 1.0,
        'composer_birth_century': 1.0,
        'soloist_type': 2.0,
        'percent_after_intermission_bin': 4.0
    }
}

FEATURES = ['work_type', 'composer_country', 'composer_birth_century', 'soloist_type', 'percent_after_intermission_bin']


def make_scorer() -> ChainEnsembleScorer:
    """get a copy of the clean, initialized scorer_template to avoid redundant computation
    ???use a shallow copy so the model object tied to the scorer is not passed around unnecessarily"""
    return deepcopy(scorer_template)


def build_program(**kwargs):
    scorer = make_scorer()

    program = None
    attempts = 1
    while program is None:
        try:
            program = scorer.generate_program(**kwargs)
        except ValueError as e:
            if attempts >= 3:
                raise e
            print(f"retrying!!! attempt #{attempts}")
        attempts += 1

    q = Session.query(Selection)

    program_order = 0
    final_program = []
    for selection in program:
        program_order += 1
        this_record = q.get(selection).to_dict()
        this_record['program_order'] = program_order
        final_program.append(this_record)

    Session.remove()
    return final_program


def make_random_params():
    # TODO: move to instance method on scorer? or maybe on optimizer class
    # scorer.make_random_params(features, max_single_weight, total_weight)

    features = FEATURES.copy()
    random.shuffle(features)

    weights = {}
    available_weight = 100

    for feature in features:
        if available_weight <= 1:
            continue
        weight_max = 65 if available_weight >= 65 else available_weight
        weight = random.randrange(1, weight_max, 1)
        weights[feature] = weight
        available_weight -= weight

    weighted_average_exponent = random.randrange(10, 30, 1) / 10
    case_weight_exponent = random.randrange(10, 30, 1) / 10
    break_weight = random.randrange(1, 20, 1)

    return {
        'feature_weights': weights,
        'weighted_average_exponent': weighted_average_exponent,
        'case_weight_exponent': case_weight_exponent,
        'break_weight': break_weight,
        'random_state': None
    }


@application.route('/rand_compare', methods=['GET'])
@cross_origin()
def rand_compare_2_programs():
    programs = []
    for _ in range(2):
        params = make_random_params()
        program = build_program(**params)
        programs.append({'selections': program, 'options': params})
    return jsonify(programs)


@application.route('/generate', methods=['GET'])
@cross_origin()
def generate():

    program_kwargs = DEFAULTS.copy()
    request_kwargs = request.get_json()
    if request_kwargs:
        for k, v in request_kwargs.items():
            program_kwargs[k] = v

    program = {
        'selections': build_program(**program_kwargs),
        'options': program_kwargs
    }

    return jsonify([program])


if __name__ == '__main__':
    application.run()
