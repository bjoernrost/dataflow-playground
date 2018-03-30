# Dataflow examples and playground

Simple dataflow streaming job that subscribes to a pub/sub topic and streams the message content to a bigquery table.

### prepare
You'll need to create a GCS storage bucket, a pub/sub topic (default is "streamdemo") and a bigquery dataset (default "demos") before running the job.

### run the job
Start a Cloud Shell, clone this repo and run the launch script, substituting your project name and storage bucket

``` sh
git clone 
cd 
./run_oncloud.sh <project-id> <storage-bucket> PubSubToBigQuery
```
### publish some test messages

Using either the console or your cloud shell, publish some messages to the topic:

```gcloud beta pubsub topics publish streamdemo --message="hello world" --project=<PROJECT-ID>
```

### query bq to see results

You can navigate to the bigquery gui and query the table from there. Note that "preview" is likely not going to work as streaming inserts take a while to populate. But results will show up for normal queries. Or query from the cli in cloud shell:

```bq query --nouse_legacy_sql 'select * from demos.wq'```
