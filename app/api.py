from fastapi import FastAPI, UploadFile, Request
from fastapi.openapi.utils import get_openapi
from fastapi.middleware.cors import CORSMiddleware
import logging
import json
import mlflow
import os
import requests
from dotenv import load_dotenv

api_app = FastAPI()
load_dotenv()

# Enable CORS
api_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@api_app.get("/")
async def root():
    return {"message": "Fraud Detection Analytics App (see /docs link)"}


@api_app.post('/inference')
async def predict(model_name, data):
    try:
        logging.info("In inference...")

        logging.error("Retrieving production model if exists...")

        model_api_uri = f'{os.getenv("MLFLOW_TRACKING_URI")}/api/2.0/mlflow/registered-models/get?name={model_name}'
        models = requests.get(model_api_uri).json()
        prod_model_name = next((x['name'] for x in models['registered_model']['latest_versions'] if
                                x['current_stage'].lower() == 'production'), None)

        if prod_model_name:
            logging.error(f"Found model {model_name} in Production stage...")

            loaded_model = mlflow.pyfunc.load_model(model_uri=f"models:/{model_name}/Production")
            unwrapped_model = loaded_model.unwrap_python_model()
            result = unwrapped_model.predict(None, data)
            logging.error(f"Result:\n{result}")

            return result
    except Exception as ee:
        logging.error("An Exception occurred...", exc_info=True)
        logging.error(str(ee))
    return "No production-ready model was found."
