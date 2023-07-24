import mlflow
import requests
from dotenv import load_dotenv
import logging
from datetime import datetime
import os

load_dotenv()


class RandomForestMadlib(mlflow.pyfunc.PythonModel):
    def predict(self, context, model_input):
        logging.info(f"Running batch prediction for offset={model_input}...")
        return self.load_gemfire_predictions(model_input)

    # TODO: Do not hardcode!
    # TODO: Integrate logic for retrieving the next training run
    def load_gemfire_predictions(self, training_run=0):
        url = f'https://gfanomaly-server.{os.getenv("DATA_E2E_BASE_URL")}/gemfire-api/v1/queries/adhoc'
        r = requests.get(url, params={
            "q": f"select distinct md.id, md.amt, md.lat, md['long'], md.is_fraud"
                 f" from /mds-region-greenplum "
                 f" where md.id!=null and md.training_run_timestamp=(select max(training_run) from /mds-region-greenplum)"
                 f" order by md.id"})
        response = r.json()
        return response


anomaly_detection_model = RandomForestMadlib()
with mlflow.start_run() as run:
    artifact_path = datetime.now().strftime("%m%d%Y%H%M%S")
    model_info = mlflow.pyfunc.log_model(artifact_path=artifact_path, python_model=anomaly_detection_model)
    mlflow.register_model(
        f"runs:/{run.info.run_id}/{artifact_path}",
        f"anomaly_detection_{artifact_path}",
        await_registration_for=None, )
