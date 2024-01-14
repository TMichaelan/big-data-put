#### Run cluster

```bash
gcloud dataproc clusters create ${CLUSTER_NAME}  --enable-component-gateway --bucket ${BUCKET_NAME}  --region ${REGION} --subnet default  --master-machine-type n1-standard-4 --master-boot-disk-size 50  --num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 50  --image-version 2.1-debian11 --optional-components JUPYTER  --project ${PROJECT_ID} --max-age=3h
```

#### Delete cluster
```bash
gcloud dataproc clusters delete ${CLUSTER_NAME} --region ${REGION} --project ${PROJECT_ID}
```