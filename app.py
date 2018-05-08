from flask import Blueprint
import subprocess
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
        list2=[]
        for key, value in feature_val.iteritems():
            if value!="":
                feat = str(key) + ":" + str(value)
                if int(key)<=9:
                    list.append(feat)
                else:
                    list2.append(feat)
                i=i+1
        list.sort()
        list2.sort()
        list+=list2
        file_name = "do_bc_predictions.txt"
        new_feature_file_to_be_predicted = open(file_name, 'w')
        new_feature_file_to_be_predicted.write(' '.join(list))
        new_feature_file_to_be_predicted.close()
        command = "hdfs dfs -rm hdfs://HadoopMaster:9000/do_bc_predictions.txt "
        subprocess.call(command, shell=True)

        command = "hdfs dfs -copyFromLocal do_bc_predictions.txt hdfs://HadoopMaster:9000/"
        subprocess.call(command, shell=True)
        result = prediction_engine.predict_bc_disease("hdfs://HadoopMaster:9000/do_bc_predictions.txt")
        r = result[0]
        command = "hdfs dfs -rm hdfs://HadoopMaster:9000/do_bc_predictions.txt "
        subprocess.call(command, shell=True)
        return render_template("bc_result.html",result = r[0])
        # return json.dumps(result)

@main.route("/ckd_result", methods=['POST', 'GET'])
def predict_ckd_disease():
    if request.method == 'POST':
        feature_val = request.form
        list = []
        list.append("0")
        i=1
        list2=[]
        for key, value in feature_val.iteritems():
            if value!="":
                feat = str(key) + ":" + str(value)
                if int(key)<=9:
                    list.append(feat)
                else:
                    list2.append(feat)
                i=i+1
        list.sort()
        list2.sort()
        list+=list2
        file_name = "do_ckd_predictions.txt"
        new_feature_file_to_be_predicted = open(file_name, 'w')
        new_feature_file_to_be_predicted.write(' '.join(list))
        new_feature_file_to_be_predicted.close()

        command = "hdfs dfs -rm hdfs://HadoopMaster:9000/do_ckd_predictions.txt "
        subprocess.call(command, shell=True)

        command = "hdfs dfs -copyFromLocal do_ckd_predictions.txt hdfs://HadoopMaster:9000/"
        subprocess.call(command, shell=True)
        result = prediction_engine.predict_ckd_disease("hdfs://HadoopMaster:9000/do_ckd_predictions.txt")
        r = result[0]
        command = "hdfs dfs -rm hdfs://HadoopMaster:9000/do_ckd_predictions.txt "
        subprocess.call(command, shell=True)
        return render_template("ckd_result.html",result = r[0])

@main.route("/diabetes_result", methods=['POST', 'GET'])
def predict_diabetes_disease():
    if request.method == 'POST':
        feature_val = request.form
        list = []
        list.append("0")
        i=1
        for key, value in feature_val.iteritems():
            if value!="":
                feat = str(key) + ":" + str(value)
                list.append(feat)
                i=i+1
        list.sort()
        file_name = "do_diabetes_predictions.txt"
        new_feature_file_to_be_predicted = open(file_name, 'w')
        new_feature_file_to_be_predicted.write(' '.join(list))
        new_feature_file_to_be_predicted.close()
        command = "hdfs dfs -rm hdfs://HadoopMaster:9000/do_diabetes_predictions.txt "
        subprocess.call(command, shell=True)

        command = "hdfs dfs -copyFromLocal do_diabetes_predictions.txt hdfs://HadoopMaster:9000/"
        subprocess.call(command, shell=True)
        result = prediction_engine.predict_diabetes_disease("hdfs://HadoopMaster:9000/do_diabetes_predictions.txt")
        r = result[0]
        command = "hdfs dfs -rm hdfs://HadoopMaster:9000/do_diabetes_predictions.txt "
        subprocess.call(command, shell=True)
        return render_template("diabetes_result.html",result = r[0])

@main.route("/blood_pressure_result", methods=['POST', 'GET'])
def predict_fembp_disease():
    if request.method == 'POST':
        feature_val = request.form
        list = []
        list.append("0")
        i=1
        list2=[]
        for key, value in feature_val.iteritems():
            if value!="":
                feat = str(key) + ":" + str(value)
                if int(key)<=9:
                    list.append(feat)
                else:
                    list2.append(feat)
                i=i+1

        list.sort()
        list2.sort()
        list+=list2
        file_name = "do_bp_predictions.txt"
        new_feature_file_to_be_predicted = open(file_name, 'w')
        new_feature_file_to_be_predicted.write(' '.join(list))
        new_feature_file_to_be_predicted.close()

        command = "hdfs dfs -rm hdfs://HadoopMaster:9000/do_bp_predictions.txt "
        subprocess.call(command, shell=True)
        command = "hdfs dfs -copyFromLocal do_bp_predictions.txt hdfs://HadoopMaster:9000/"
        subprocess.call(command, shell=True)
        result = prediction_engine.predict_fembp_disease("hdfs://HadoopMaster:9000/do_bp_predictions.txt")
        r = result[0]
        command = "hdfs dfs -rm hdfs://HadoopMaster:9000/do_bp_predictions.txt "
        subprocess.call(command, shell=True)
        return render_template("blood_pressure_result.html",result = r[0])

@main.route("/heart_result", methods=['POST', 'GET'])
def predict_heart_disease():
    if request.method == 'POST':
        feature_val = request.form
        list = []
        list.append("0")
        i=1
        list2=[]
        for key, value in feature_val.iteritems():
            if value!="":
                feat = str(key) + ":" + str(value)
                if int(key)<=9:
                    list.append(feat)
                else:
                    list2.append(feat)
                i=i+1
        list.sort()
        list2.sort()
        list+=list2
        file_name = "do_heart_predictions.txt"
        new_feature_file_to_be_predicted = open(file_name, 'w')
        new_feature_file_to_be_predicted.write(' '.join(list))
        new_feature_file_to_be_predicted.close()

        command = "hdfs dfs -rm hdfs://HadoopMaster:9000/do_heart_predictions.txt "
        subprocess.call(command, shell=True)

        command = "hdfs dfs -copyFromLocal do_heart_predictions.txt hdfs://HadoopMaster:9000/"
        subprocess.call(command, shell=True)
        result = prediction_engine.predict_heart_disease("hdfs://HadoopMaster:9000/do_heart_predictions.txt")
        r = result[0]
        command = "hdfs dfs -rm hdfs://HadoopMaster:9000/do_heart_predictions.txt "
        subprocess.call(command, shell=True)
        return render_template("heart_result.html",result = r[0])

@main.route("/bc_feature")
def choose_breast_cancer_features():
   return render_template('bc_feature_1.html')

@main.route("/ckd_features")
def choose_ckd_features():
   return render_template('ckd_features.html')

@main.route("/diabetes_feature")
def choose_diabetes_features():
   return render_template('diabetes_feature.html')

@main.route("/blood_pressure_features")
def choose_fembp_features():
   return render_template('blood_pressure_features.html')

@main.route("/heart_disease_feature")
def choose_heart_features():
   return render_template('heart_disease_features.html')

@main.route("/")
def load_home_page():
   return render_template('main.html')

@main.route("/select")
def load_select_page():
   return render_template('select.html')

def create_app(spark_context):
    global prediction_engine

    prediction_engine = PredictionEngine(spark_context)

    app = Flask(__name__)
    app.register_blueprint(main)
    return app