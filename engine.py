import os
from pyspark.mllib.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PredictionEngine:
    """A disease prediction engine
    """
    def predict_bc_disease(self, breast_cancer_pred_path):

        data = self.spark.read.format("libsvm").load(breast_cancer_pred_path)
        result = self.bcmodel.transform(data)
        r=result.select("prediction")
        return r.collect()

    def predict_ckd_disease(self, ckd_pred_path):
        data = self.spark.read.format("libsvm").load(ckd_pred_path)
        result = self.ckdmodel.transform(data)
        r = result.select("prediction")
        return r.collect()

    def predict_diabetes_disease(self, diabetes_pred_path):
        data = self.spark.read.format("libsvm").load(diabetes_pred_path)
        result = self.diabetesmodel.transform(data)
        r = result.select("prediction")
        return r.collect()

    def predict_fembp_disease(self, fembp_pred_path):
        data = self.spark.read.format("libsvm").load(fembp_pred_path)
        result = self.fembpmodel.transform(data)
        r = result.select("prediction")
        return r.collect()

    def predict_heart_disease(self, heart_pred_path):
        data = self.spark.read.format("libsvm").load(heart_pred_path)
        result = self.heartmodel.transform(data)
        r = result.select("prediction")
        return r.collect()

    def __init__(self, sc):
        """Init the recommendation engine given a Spark context and a dataset path
        """
        logger.info("Starting up the Prediction Engine: ")

        self.sc = sc
        self.bcmodel = PipelineModel.load("hdfs://HadoopMaster:9000/DT_Breast_Cancer")
        self.ckdmodel=PipelineModel.load("hdfs://HadoopMaster:9000/user/beuser/DT_CKD")
        self.diabetesmodel=PipelineModel.load("hdfs://HadoopMaster:9000/user/beuser/DT_Diabetes")
        self.fembpmodel=PipelineModel.load("hdfs://HadoopMaster:9000/user/beuser/DT_femBP")
        self.heartmodel=PipelineModel.load("hdfs://HadoopMaster:9000/user/beuser/DT_heart")
        self.spark=SparkSession(sc)
