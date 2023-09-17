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

load_dotenv()


class RandomForestMadlib(mlflow.pyfunc.PythonModel):
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


# TODO: Refactor tracking logic!
def publish():
    anomaly_detection_model = RandomForestMadlib()
    client = MlflowClient()

    with mlflow.start_run() as run:
        ################################################
        # track relevant metrics
        ################################################
        try:
            logging.error("about to track...")
            metrics_data = _track_metrics()
            mlflow.log_dict(metrics_data.to_dict(), 'importances.json')
        except Exception as ee:
            logging.error("An Exception occurred...", exc_info=True)
            logging.error(str(ee))
            logging.error(''.join(traceback.TracebackException.from_exception(ee).format()))

        ################################################
        # set tags
        ################################################
        artifact_path = datetime.now().strftime("%m%d%Y%H%M%S")
        model_name = f"anomaly_detection_{artifact_path}"
        client.set_registered_model_tag(model_name, 'group', 'anomaly_detection')

        ################################################
        # publish model
        ################################################
        mlflow.pyfunc.log_model(artifact_path=artifact_path, python_model=anomaly_detection_model)
        model_path = f"runs:/{run.info.run_id}/{artifact_path}"
        mlflow.register_model(
            model_path,
            model_name,
            await_registration_for=None, )
        return model_path


# TODO: Do not hardcode URI or query!
def _track_metrics():
    cnx = create_engine('postgresql://gpadmin:Uu4jcDSjqlDVQ@44.201.91.88:5432/dev?sslmode=require')
    # Get variable importances
    df = pd.read_sql_query("SELECT * FROM rf_credit_card_transactions_importances ORDER BY oob_var_importance DESC", cnx)
    logging.error(df)
    return df


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


publish()
