import java.io.File
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext, SparkSession}
import play.api.libs.json._

import scala.collection.immutable.Seq
import scala.collection.mutable.ArrayBuffer
import collection.JavaConverters._

/**
  * Scans directory with files containing pairs of username:password
  *
  * This application listens on a topic "databreachToScan" in a local kafka cluster
  * and returns
  */
object DataBreachScannerService {

  var m: Seq[(String, String)] = Seq[(String, String)]()

  val breaches: Seq[(String, String)] = Seq(
    ("ExploitIn", "/opt/databreach")
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Databreach Scanner Service")
      .getOrCreate()

    /**
      * Create a RDD out of all files in the directory /opt/databreach
      */
    val breachRDD: RDD[(String, String)] = spark.sparkContext.textFile(s"${breaches.head._2}/*.txt").map {
      row: String => {
        try {
          //split the email and the lastname
          val pair = row.split(":", 2)
          (pair(0), pair(1))
        } catch {
          case e: ArrayIndexOutOfBoundsException => ("", "")
        }
      }
        //filter out emtpy lines and lines containing no pw or no email
    }.filter(s => !s._1.isEmpty && !s._2.isEmpty)


    val consumer = getConsumer
    val producer = getProducer

    /**
      * Adding Shutdown Hooks to close producer and consumer on application shutdown
      */
    sys.addShutdownHook(consumer.close())
    sys.addShutdownHook(producer.close())

    //random generator for the event key
    val r = scala.util.Random

    //subscripe the topic
    consumer.subscribe(ArrayBuffer("databreachToScan").asJava);

    //endless loop
    while (true) {
      //last offset of the consumer
      var lastOffset: Long = 0;
      //getting records
      val records: ConsumerRecords[String, String] = consumer.poll(100);

      //iterating over the records
      for (record: ConsumerRecord[String, String] <- records.asScala) {
        println(s"offset = ${record.offset()}, key = ${record.key}, value = ${record.value()}");
        val json = Json.parse(record.value) //parse record value as json
        val keyword = (json \ "keyword").as[String] //extract keyword
        val url = (json \ "url").as[String] //extract url
        val name =  (json \ "name").as[String] //extract name
        val pattern = s".*$name.*" // the pattern to use for the matching

        //filter for matches and prepare json format
        val result = breachRDD.filter(_._1.matches(pattern)).map {
          matching => s"""{ "email": "${matching._1}",  "password": "${matching._2}"}"""
        }.collect()
          .mkString("[", ",", "]") // create the list json structure


        //the final json
        val value = s"""{ "keyword": "$keyword", "results": { "url": "$url", "name": "$name", "results": $result }}"""

        //creating the producer record
        val producerRecord = new ProducerRecord[String, String](
          "databreachScannerUpdate", //topic name
          r.nextString(16),  //key of record
          value // the json msg we want to send
        )
        producer.send(producerRecord) // send the record
        lastOffset = record.offset() // get offset
        println(s"Last Offset: $lastOffset")
      }
      consumer.commitSync() //commit offset
    }
  }

  /**
    * Creates the kafka consumer with at least once
    * @return
    */
  def getConsumer: KafkaConsumer[String, String] = {
    val consumerProps: Properties = new Properties();
    consumerProps.put("bootstrap.servers", "localhost:9092")  //kafka url
    consumerProps.put("group.id", "DataBreachScannerService") //name of the group
    consumerProps.put("enable.auto.commit", "false") //required for at least once
    consumerProps.put("request.timeout.ms", "120000") //the timeout of the consumer need to be higher then the processing time
    consumerProps.put("session.timeout.ms", "100000") //need to be lower then the request timeout
    consumerProps.put("fetch.max.wait.ms", "100000")//need to be lower then the request timeout
    consumerProps.put("heartbeat.interval.ms", "3000")
    consumerProps.put("auto.offset.reset", "earliest") //taking the earliest offset
    consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    new KafkaConsumer[String, String](consumerProps);
  }

  def getProducer: KafkaProducer[String, String] = {
    val producerProps = new Properties()
    producerProps.put("bootstrap.servers", "localhost:9092") //kafka url
    producerProps.put("acks", "all")
    producerProps.put("retries", "0")
    producerProps.put("batch.size", "16384") //the size of batches
    producerProps.put("linger.ms", "1")
    producerProps.put("buffer.memory", "33554432")
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](producerProps)
  }
}

