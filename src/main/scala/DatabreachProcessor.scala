import java.io.File
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext, SparkSession}
import org.apache.spark.streaming._
import play.api.libs.json._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.KafkaUtils

import scala.collection.immutable.Seq
import scala.io.Source


object DataBreachProcessor {
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


    val file = new File("/tmp/test.file")
    if(file.exists()) {
      val breachDF: RDD[(String, String)] = spark.sparkContext.textFile(s"/Users/David/Documents/Exploit.in/11*.txt").map {
        row: String => {
          try {
            val pair = row.split(":", 2)
            (pair(0), pair(1))
          } catch {
            case e: ArrayIndexOutOfBoundsException => ("", "")
          }
        }
      }.filter(s => !s._1.isEmpty && !s._2.isEmpty)

      val producerProps = new Properties()
      producerProps.put("bootstrap.servers", "localhost:9092")
      producerProps.put("acks", "all")
      producerProps.put("retries", "0")
      producerProps.put("batch.size", "16384")
      producerProps.put("linger.ms", "1")
      producerProps.put("buffer.memory", "33554432")
      producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](producerProps)
      val src = Source.fromFile(file)

      for(line <- src.getLines()) {
        println(line)
        val Array(firstname, lastname, url, keyword) = line.split("<:>", 4)

        val pattern = s"(.*(${firstname})+.*(${lastname})*|.*(${firstname})+.*(${lastname})*|.*(${firstname.charAt(0)}${lastname.charAt(0)})+.*)"
        val result = breachDF.filter(_._1.matches(pattern)).map {
          matching => s"""{ "email": "${matching._1}",  "password": "${matching._2}"}"""
        }.collect().mkString("[", ",", "]")

        val value = s"""{ "keyword": "$keyword", "results": { "url": "$url", "firstname": "$firstname", "lastname": "$lastname", "scanned": true, "results": $result }}"""
        val producerRecord = new ProducerRecord[String, String](
          "databreachScannerUpdate",
          keyword,
          value
        )
        producer.send(producerRecord)
      }
      producer.close()
      src.close()
      file.delete()
    } else {
      val producerProps = new Properties()
      producerProps.put("bootstrap.servers", "localhost:9092")
      producerProps.put("acks", "all")
      producerProps.put("retries", "0")
      producerProps.put("batch.size", "16384")
      producerProps.put("linger.ms", "1")
      producerProps.put("buffer.memory", "33554432")
      producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](producerProps)
      val producerRecord = new ProducerRecord[String, String](
        "databreachScannerUpdate",
        "test",
       s"""{ "keyword": "test", "results": { "url": "test", "firstname": "test", "lastname": "test", "scanned": true, "results": [] }}"""
      )
      producer.send(producerRecord)
    }

  }
}


