###Starting Spark Application
```bash
spark-submit --packages com.typesafe.play.play-json_2.11:2.6.0,org.apache.spark:spark-streaming-kafka-0-10_2.11:2.2.0 --class "DatabreachProcessor" target/scala-2.11/databreach-processor_2.11-1.0-M1.jar
```
