import mlflow
from mlflow import MlflowClient
import requests
from dotenv import load_dotenv
import logging
from datetime import datetime
import os
import pandas as pd
from sqlalchemy import create_engine
import traceback
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from skl2onnx import to_onnx
import notifications
import publish_metadata
import pika

load_dotenv()


class StreamingRandomForestMadlib(mlflow.pyfunc.PythonModel):

    def predict(self, context, model_input):
        logging.info(f"Running batch prediction for offset={model_input}...")
        return self.load_gemfire_predictions(model_input)

    # TODO: Do not hardcode!
    # TODO: Integrate logic for retrieving the next training run
    def load_gemfire_predictions(self, model_input):
        training_run = _get_last_offset(region='mds_region_greenplum_offset')

        url = f'{_get_gemfire_api_endpoint()}/queries/adhoc'

        r = requests.get(url, params={
            "q": f"select distinct id, amount, latitude, longitude, is_fraud_flag"
                 f" from /mds-region-greenplum "
                 f" where id!=null and training_run_timestamp > {training_run}"
                 f" order by id"
                 f" limit 100"})

        # TODO: Set value of new offset

        response = r.json()
        return response


class RandomForestMadlib(mlflow.pyfunc.PythonModel):
    def predict(self, context, model_input):
        logging.info(f"Running prediction for inputs={model_input}...")
        return self.run_inference(model_input)

    # TODO: Use network local address instead of Loadbalancer FQDN for db connection (for performance reasons)
    # TODO: Integrate logic for retrieving the next training run
    # TODO: Include error handling
    def run_inference(self, model_input):
        inference_function_name = 'run_random_forest_prediction'
        cnx = create_engine(os.getenv('inference_db_uri_full'))
        logging.error(f"Training db is {os.getenv('training_db_uri_full')}")
        df = pd.read_sql_query(
            f"SELECT {inference_function_name}({model_input[0]}, {model_input[1]}, {model_input[2]}, {model_input[3]})",
            cnx)
        result = df[inference_function_name].iloc[0]
        return result


# TODO: Do not hardcode!
# TODO: Use network local address instead of Loadbalancer FQDN for db connection (for performance reasons)
# TODO: Integrate logic for retrieving the next training run
# TODO: Include error handling
class RandomForestMadlibOnnx(mlflow.pyfunc.PythonModel):
    def __init__(self):
        cnx = create_engine(os.getenv('training_db_uri_full'))
        logging.error(f"Training db is {os.getenv('training_db_uri_full')}")
        df = pd.read_sql_query(f"select * from \"rf_credit_card_transactions_training\"", cnx)
        X, y = df[["time_elapsed", "amt", "lat", "long"]].to_numpy(), df[["is_fraud"]].to_numpy()
        X_train, X_test, y_train, y_test = train_test_split(X, y)
        model = RandomForestClassifier(random_state=1, n_estimators=5, class_weight='balanced')
        model.fit(X_train, y_train)
        self.model = model

        ################################################
        # log explanations
        ################################################
        # mlflow.shap.log_explanation(model.predict, X_train)

        onx = to_onnx(self.model, X[:1])
        artifact_name = "rf_fraud.onnx"
        with open(artifact_name, "wb") as f:
            f.write(onx.SerializeToString())
        self.artifact_name = artifact_name

    # TODO: Implement
    def predict(self, context, model_input):
        logging.info(f"Running prediction for inputs={model_input}...")
        return self.run_inference(model_input)

    def run_inference(self, model_input):
        try:
            logging.error("about to track...")
            metrics_data = _track_metrics()
            mlflow.log_dict(metrics_data[0].to_dict(), 'importances.json')
            mlflow.log_dict(metrics_data[1].to_dict(), 'oob.json')
        except Exception as ee:
            logging.error("An Exception occurred...", exc_info=True)
            logging.error(str(ee))
            logging.error(''.join(traceback.TracebackException.from_exception(ee).format()))
        return None


# TODO: Refactor tracking logic!
def publish_streaming_model():
    model = StreamingRandomForestMadlib()
    return publish(model, 'anomaly_detection_streaming')


# TODO: Refactor tracking logic!
def publish_single_record_model():
    model = RandomForestMadlib()
    return publish(model, 'anomaly_detection')


# TODO: Refactor tracking logic!
def publish_single_record_model_onnx():
    model = RandomForestMadlibOnnx()
    return publish(model, 'anomaly_detection_onnx')


def publish(model, model_name_prefix):
    client = MlflowClient()

    with mlflow.start_run() as run:
        artifact_path = datetime.now().strftime("%m%d%Y%H%M%S")
        ################################################
        # track relevant metrics
        ################################################
        try:
            logging.error("about to track...")
            metrics_data = _track_metrics()
            mlflow.log_dict(metrics_data[0].to_dict(), 'importances.json')
            mlflow.log_dict(metrics_data[1].to_dict(), 'oob.json')
            if hasattr(model, 'artifact_name'):
                mlflow.log_artifact(model.artifact_name, artifact_path=artifact_path)

        except Exception as ee:
            logging.error("An Exception occurred...", exc_info=True)
            logging.error(str(ee))
            logging.error(''.join(traceback.TracebackException.from_exception(ee).format()))

        ################################################
        # publish model
        ################################################
        model_name = f"{model_name_prefix}_{artifact_path}"
        mlflow.pyfunc.log_model(artifact_path=artifact_path, python_model=model)
        model_path = f"runs:/{run.info.run_id}/{artifact_path}"
        mlflow.register_model(
            model_path,
            model_name,
            await_registration_for=None, )

        ################################################
        # publish metadata
        ################################################
        try:
            publish_metadata.publish_postgres_training_db()
            publish_metadata.publish_inference_training_db()
            publish_metadata.publish_gemfire_inference_db()
        except Exception as ee:
            logging.error("An Exception occurred...", exc_info=True)
            logging.error(str(ee))
            logging.error(''.join(traceback.TracebackException.from_exception(ee).format()))

        ################################################
        # set tags
        ################################################
        prior_runs = mlflow.search_runs(
            filter_string=f"tags.latest = 'true' AND tags.type = '{model_name_prefix}'",
            output_format='list')
        for prior_run in prior_runs:
            client.delete_tag(prior_run.info.run_id, 'latest')
        mlflow.set_tag('type', model_name_prefix)
        mlflow.set_tag('latest', 'true')
        client.set_registered_model_tag(model_name, 'group', 'anomaly_detection')
        return model_path


# TODO: Do not hardcode query!
def notify_completion():
    logging.info(f"Sending notification of model training completion to {os.getenv('rmq_host')}...")
    prior_runs = mlflow.search_runs(
        filter_string=f"tags.latest = 'true' AND tags.type ILIKE 'anomaly_detection%'",
        output_format='list')
    notifications.send_argo_event('{"message": "sync"}')


# TODO: Do not hardcode query!
def _track_metrics():
    cnx = create_engine(os.getenv('training_db_uri_full'))
    # Get variable importances
    df_importances = pd.read_sql_query(
        "SELECT * FROM rf_credit_card_transactions_importances ORDER BY oob_var_importance DESC",
        cnx)
    df_oob_error = pd.read_sql_query(
        "SELECT avg(oob_error) FROM rf_credit_card_transactions_model_group",
        cnx)
    logging.error(f"{df_importances} {df_oob_error}")
    return df_importances, df_oob_error


def _get_last_offset(region: str) -> int:
    try:
        url = f"{_get_gemfire_api_endpoint()}/{region}/offset"
        r = requests.get(url)
        return int(r.text)
    except Exception as e:
        logging.error("Offset not yet initialized.")
        result = 0
        url = f"{_get_gemfire_api_endpoint()}/{region}?key=offset"
        requests.post(url, data=str(result).encode('utf-8'))
        return result


def _set_last_offset(region: str, offset: int):
    try:
        url = f"{_get_gemfire_api_endpoint()}/{region}/offset"
        requests.put(url, data=str(offset).encode('utf-8'))
    except Exception as e:
        logging.error("ERROR: Could not set offset...")


def _get_gemfire_api_endpoint():
    return f'https://gfanomaly-server.{os.getenv("DATA_E2E_BASE_URL")}/gemfire-api/v1'


publish_streaming_model()
publish_single_record_model()
publish_single_record_model_onnx()
notify_completion()
