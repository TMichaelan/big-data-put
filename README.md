# big-data-put

## Preparing
Export the bucket name
```bash
export BUCKET_NAME=YOUR_BUCKET_NAME
```

### Run MapReduce


```bash
mapred streaming -files mapper.py,combiner.py,reducer.py -input gs://${BUCKET_NAME}/projekt1/input/datasource1 -mapper  mapper.py -combiner combiner.py -reducer reducer.py -output gs://${BUCKET_NAME}/projekt1/mr_output
```

### Run Hive
```bash
beeline -u jdbc:hive2://localhost:10000/default \
      -f hive.hql \
      --hivevar input_dir4=gs://${BUCKET_NAME}/projekt1/input/datasource4 \
      --hivevar input_dir3=gs://${BUCKET_NAME}/projekt1/mr_output \
      --hivevar output_dir6=gs://${BUCKET_NAME}/projekt1/hive_output
```
