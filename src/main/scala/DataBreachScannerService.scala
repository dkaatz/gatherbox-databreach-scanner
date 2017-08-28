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


object DataBreachScannerService {

  var m: Seq[(String, String)] = Seq[(String, String)]()

  val breaches: Seq[(String, String)] = Seq(
    ("ExploitIn", "/Users/David/Documents/Exploit.in")
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Databreach Scanner Service")
      .master("local[4]")
      .getOrCreate()

    val breachRDD: RDD[(String, String)] = spark.sparkContext.textFile(s"${breaches.head._2}/*.txt").map {
      row: String => {
        try {
          val pair = row.split(":", 2)
          (pair(0), pair(1))
        } catch {
          case e: ArrayIndexOutOfBoundsException => ("", "")
        }
      }
    }.filter(s => !s._1.isEmpty && !s._2.isEmpty)


    val consumer = getConsumer
    val producer = getProducer

    /**
      * Adding Shutdown Hooks to close producer and consumer on application shutdown
      */
    sys.addShutdownHook(consumer.close())
    sys.addShutdownHook(producer.close())

    val r = scala.util.Random
    consumer.subscribe(ArrayBuffer("databreachToScan").asJava);
    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(100);
      for (record: ConsumerRecord[String, String] <- records.asScala) {
        println(s"offset = ${record.offset()}, key = ${record.key}, value = ${record.value()}");
        val json = Json.parse(record.value)
        val keyword = (json \ "keyword").as[String]
        val url = (json \ "url").as[String]
        val name =  (json \ "name").as[String]
        val pattern = s".*$name.*"
        val result = breachRDD.filter(_._1.matches(pattern)).map {
          matching => s"""{ "email": "${matching._1}",  "password": "${matching._2}"}"""
        }.collect().mkString("[", ",", "]")

        val value = s"""{ "keyword": "$keyword", "results": { "url": "$url", "name": "$name", "results": $result }}"""
        val producerRecord = new ProducerRecord[String, String](
          "databreachScannerUpdate",
          r.nextString(16),
          value
        )
        producer.send(producerRecord)
      }
      consumer.commitSync()
    }
  }

  def getConsumer: KafkaConsumer[String, String] = {
    val consumerProps: Properties = new Properties();
    consumerProps.put("bootstrap.servers", "localhost:9092");
    consumerProps.put("group.id", "DataBreachScannerService");
    consumerProps.put("enable.auto.commit", "false");
    consumerProps.put("session.timeout.ms", "30000");
    consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    new KafkaConsumer[String, String](consumerProps);
  }

  def getProducer: KafkaProducer[String, String] = {
    val producerProps = new Properties()
    producerProps.put("bootstrap.servers", "localhost:9092")
    producerProps.put("acks", "all")
    producerProps.put("retries", "0")
    producerProps.put("batch.size", "16384")
    producerProps.put("linger.ms", "1")
    producerProps.put("buffer.memory", "33554432")
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](producerProps)
  }
}

