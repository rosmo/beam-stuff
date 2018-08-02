# beam-stuff
Apache Beam &amp; Dataflow stuff

### Running the Dataflow-job:

```
mvn compile exec:java \
    -Dexec.mainClass=me.nordiccloudteam.ImportCitybikesData \
    -Dexec.args="--output=your-project:bigquery.table --input=gs://your-bucket/data/*.json  --stagingLocation=gs://your-dataflow-bucket/staging --tempLocation=gs://your-dataflow-bucket/temp --runner=DataflowRunner"
```