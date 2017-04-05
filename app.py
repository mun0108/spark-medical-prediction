from flask import Blueprint

main = Blueprint('main', __name__)

import json
from engine import PredictionEngine

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request, redirect, url_for,render_template


@main.route("/bc_result", methods=['POST', 'GET'])
def predict_disease():
    if request.method == 'POST':

        feature_val = request.form
        list = []
        list.append("0")
        i=1
        for key, value in feature_val.iteritems():
            feat = str(i) + ":" + str(value)
            list.append(feat)
            i=i+1
        file_name = "do_predictions.txt"
        new_feature_file_to_be_predicted = open(file_name, 'w')
        new_feature_file_to_be_predicted.write(' '.join(list))
        new_feature_file_to_be_predicted.close()
        result=prediction_engine.predict_bc_disease(file_name)
        r=result[0]

        return render_template("result.html",result = r[0])
        # return json.dumps(result)


@main.route("/")
def choose_breast_cancer_features():
   return render_template('bc_feature.html')

def create_app(spark_context):
    global prediction_engine

    prediction_engine = PredictionEngine(spark_context)

    app = Flask(__name__)
    app.register_blueprint(main)
    return app