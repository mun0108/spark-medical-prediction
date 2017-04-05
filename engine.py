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


    def __init__(self, sc):
        """Init the recommendation engine given a Spark context and a dataset path
        """
        logger.info("Starting up the Prediction Engine: ")

        self.sc = sc
        self.bcmodel = PipelineModel.load("DT_Breast_Cancer")

        self.spark=SparkSession(sc)

