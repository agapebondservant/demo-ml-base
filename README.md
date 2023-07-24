* Build the images via docker:
```
source .env
docker build --build-arg MLFLOW_TRACKING_URI_VAL=http://mlflow.${DATA_E2E_BASE_URL} \
             --build-arg MLFLOW_S3_ENDPOINT_URL_VAL=http://minio-ml.${DATA_E2E_BASE_URL} \
             -t ${DATA_E2E_REGISTRY_USERNAME}/demo-ml-base .
docker push ${DATA_E2E_REGISTRY_USERNAME}/demo-ml-base
```

## Deploying ML Inference API
* Deploy the API app:
```
source .env
envsubst < config/workload.in.yaml > config/workload.yaml
tanzu apps workload create anomaly-detection -f config/workload.yaml --yes
```

* Tail the logs of the API app:
```
tanzu apps workload tail anomaly-detection --since 64h
```

* Once deployment succeeds, get the URL for the API app:
```
tanzu apps workload get anomaly-detection     #should yield anomaly-detection.default.<your-domain>
```

* To delete the app:
```
tanzu apps workload delete anomaly-detection --yes
```
