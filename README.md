* Build the images via docker:
```
source .env
docker build --build-arg MLFLOW_TRACKING_URI_VAL=http://mlflow.${DATA_E2E_BASE_URL} \
             --build-arg MLFLOW_S3_ENDPOINT_URL_VAL=http://minio-ml.${DATA_E2E_BASE_URL} \
             -t ${DATA_E2E_REGISTRY_USERNAME}/demo-ml-base .
docker push ${DATA_E2E_REGISTRY_USERNAME}/demo-ml-base
```
