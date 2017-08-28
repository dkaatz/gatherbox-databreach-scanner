import java.io.File
import java.util.Properties

import kafka.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext, SparkSession}
import org.apache.spark.streaming._
import play.api.libs.json._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.KafkaUtils

import scala.collection.immutable.Seq
import scala.util.control.Breaks._


object DataBreachReceiver {
  var m: Seq[(String, String)] = Seq[(String, String)]()

  val breaches: Seq[(String, String)] = Seq(
    ("ExploitIn", "/Users/David/Documents/Exploit.in")
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Databreach Processor")
      .master("local[2]")
      .getOrCreate()

    //create streaming contect
    val ssc = new StreamingContext(spark.sparkContext, Seconds(2))
    //topics to subscribe
    val topics = Set("databreachToScan").toSet

    //kafka consumer configuration
    val kafkaParams = Map[String, Object](
      "metadata.broker.list" -> "0.0.0.0:9092",
      "bootstrap.servers" -> "0.0.0.0:9092",
      "group.id" -> "databreachScannerService",
      "auto.commit.interval.ms" -> "1000",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> "true"
    )

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    messages.foreachRDD { messageRdd => {
        println("# events = " + messageRdd.count())
        if(!messageRdd.isEmpty())
          messageRdd.map {
            record =>
              val json = Json.parse(record.value)
              val keyword = (json \ "keyword").as[String]
              val url = (json \ "url").as[String]
              val firstname =  (json \ "firstname").as[String]
              val lastname =  (json \ "lastname").as[String]
              s"""$firstname<:>$lastname<:>$url<:>$keyword"""
          }.saveAsTextFile("/tmp/test.file")
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}

