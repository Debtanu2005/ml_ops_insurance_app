from src.configuration.mongo_db_connection import MongoDBClient
from src.entity.config_entity import data_ingestion_config
from src.logger import logging
from src.exception import MyException
from src.entity.artifact_entity import DataIngestionArtifact
from src.constants import *

import sys
import os
import numpy as np
import pandas as pd
from pandas import DataFrame
from sklearn.model_selection import train_test_split


class DataIngestion:

    def __init__(self, data_ingestion_config=data_ingestion_config):
        try:
            logging.info(f"{'>>'*20} Data Ingestion Started {'<<'*20}")
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise MyException(e, sys)

    def export_data_into_feature_store(self) -> DataFrame:
        try:
            logging.info("Connecting to MongoDB")

            mongo_client = MongoDBClient()
            collection = mongo_client.database[DATA_INGESTION_COLLECTION_NAME]

            document_count = collection.count_documents({})
            logging.info(f"MongoDB document count: {document_count}")

            if document_count == 0:
                raise ValueError("MongoDB collection is empty")

            data = list(collection.find())
            df = pd.DataFrame(data)

            logging.info(f"Fetched dataframe shape: {df.shape}")

            # Drop MongoDB internal column
            if "_id" in df.columns:
                df.drop(columns=["_id"], inplace=True)

            df.replace({"na": np.nan}, inplace=True)

            return df

        except Exception as e:
            raise MyException(e, sys)

    def split_data_as_train_test(self, dataframe: DataFrame) -> None:
        try:
            logging.info("Splitting data into train and test sets")

            if dataframe.shape[0] == 0:
                raise ValueError("Cannot split empty dataframe")

            train_set, test_set = train_test_split(
                dataframe,
                test_size=self.data_ingestion_config.train_test_split_ratio,
                random_state=42
            )

            dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(dir_path, exist_ok=True)

            train_set.to_csv(
                self.data_ingestion_config.training_file_path,
                index=False,
                header=True
            )

            test_set.to_csv(
                self.data_ingestion_config.testing_file_path,
                index=False,
                header=True
            )

            logging.info("Train-test split completed successfully")

        except Exception as e:
            raise MyException(e, sys)

    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        try:
            logging.info("Initiating data ingestion process")

            dataframe = self.export_data_into_feature_store()
            self.split_data_as_train_test(dataframe)

            data_ingestion_artifact = DataIngestionArtifact(
                trained_file_path=self.data_ingestion_config.training_file_path,
                test_file_path=self.data_ingestion_config.testing_file_path
            )

            logging.info(f"Data ingestion artifact created: {data_ingestion_artifact}")
            return data_ingestion_artifact

        except Exception as e:
            raise MyException(e, sys)
