from datahub.ingestion.run.pipeline import Pipeline
import os
from dotenv import load_dotenv
import logging

load_dotenv()


def publish_postgres_training_db():
    logging.info(f"Publishing training metadata to Datahub at {os.getenv('datahub_rest_uri')}...")
    pipeline = Pipeline.create(
        {
            "source": {
                "type": "postgres",
                "config": {
                    "username": os.getenv('training_user'),
                    "password": os.getenv('training_password'),
                    "database": os.getenv('training_db_name'),
                    "host_port": f"{os.getenv('training_master')}:5432",
                },
            },
            "sink": {
                "type": "datahub-rest",
                "config": {"server": f"http://{os.getenv('datahub_rest_uri')}"},
            },
        }
    )

    pipeline.run()
    pipeline.pretty_print_summary()


def publish_postgres_inference_db():
    logging.info(f"Publishing inference metadata to Datahub at {os.getenv('datahub_rest_uri')}...")
    pipeline = Pipeline.create(
        {
            "source": {
                "type": "postgres",
                "config": {
                    "username": os.getenv('inference_user'),
                    "password": os.getenv('inference_password'),
                    "database": os.getenv('inference_db_name'),
                    "host_port": f"{os.getenv('inference_host')}:5432",
                },
            },
            "sink": {
                "type": "datahub-rest",
                "config": {"server": f"http://{os.getenv('datahub_rest_uri')}"},
            },
        }
    )

    pipeline.run()
    pipeline.pretty_print_summary()

