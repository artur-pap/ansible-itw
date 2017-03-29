package com.epam.itw.processors

import java.util.{Date, Properties}

import com.epam.itw.kafka.KafkaWriter._
import com.epam.itw.utils.Utilities
import com.typesafe.config.ConfigFactory
import io.circe.generic.auto._
import io.circe.syntax._
import net.ceedubs.ficus.Ficus._
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization._
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{StreamingContext, _}

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

@SerialVersionUID(170L)
class InputRow(var requestDateTime: Long,
               var subscriberID: String,
               var downstreamSystem: String,
               var requestUrl: String,
               var latency: Long,
               var serverNumber: Int
              ) extends Serializable {

  override def toString: String = s"time: ${new Date(requestDateTime)}; subscriberID: $subscriberID; downstreamSystem: $downstreamSystem; requestUrl: $requestUrl; latency: $latency"

  def valid(): Boolean = requestDateTime != -1L
}

object InputRow {
  val RequestDateIndex = 0
  val RequestTimeIndex = 1
  val SubscriberIdIndex = 9
  val RequestUrlIndex = 11
  val LatencyIndex = 14

  val requestDelimiter = ":\\d{2,5}"

  val continuousSpaceDelimiter: Regex = "[\\ ]{1,}".r
  val dateTimeFormatString = "yyyy/MM/dd HH:mm:ss.SSS"

  def apply(environment: String, message: String, source: String): InputRow = {
    val messageArray = message.split(continuousSpaceDelimiter.regex)

    val requestDateTimeString = messageArray(RequestDateIndex).concat(" ").concat(messageArray(RequestTimeIndex))
    val requestDateTimeLong = Utilities.parseTime8toUTC(requestDateTimeString, dateTimeFormatString)

    val subscriberID = messageArray(SubscriberIdIndex)

    val downstreamSystem = messageArray(RequestUrlIndex).split(requestDelimiter)(0)
    val requestUrl = messageArray(RequestUrlIndex).split(requestDelimiter)(1)
    val latency = messageArray(LatencyIndex).split(":")(1).toLong
    val serverNumber = Utilities.getLogServerNumber(source)

    new InputRow(requestDateTimeLong, subscriberID, downstreamSystem, requestUrl, latency, serverNumber)
  }

  def apply(): InputRow = new InputRow(-1L, "", "", "", 0L, -1)
}

object SomeProcessor {
  def main(args: Array[String]): Unit = {
    LogManager.getRootLogger.setLevel(Level.WARN)

    val config = ConfigFactory.load()

    val percentilesParam = ""
    val kafkaServers = Try(args(0)).getOrElse(sys.env.getOrElse("KAFKA_SERVERS", config.as[String]("processor.kafka-servers")))
    val batchDuration = Try(args(1)).getOrElse(sys.env.getOrElse("BATCH_DURATION", config.as[String]("processor.batch-duration"))).toLong
    val inputTopic = Try(args(2)).getOrElse(sys.env.getOrElse("INPUT_TOPIC", config.as[String]("processor.topics.input")))
    val outputTopic = Try(args(3)).getOrElse(sys.env.getOrElse("OUTPUT_TOPIC", config.as[String]("processor.topics.output")))
    val incomingFilterPattern = Try(args(4)).getOrElse(sys.env.getOrElse("MSG_FILTER", config.as[String]("processor.incoming-filter-pattern")))

    println(s"kafkaServers $kafkaServers")
    println(s"batchDuration $batchDuration")
    println(s"inputTopic $inputTopic")
    println(s"outputTopic $outputTopic")
    println(s"incomingFilterPattern $incomingFilterPattern")

    val kafkaGroupId = "some-kafka-groups-id"

    val producerConfig = {
      val p = new Properties()
      p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
      p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.StringSerializer].getName)
      p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.StringSerializer].getName)
      p
    }

    def createContext(): StreamingContext = {
      implicit val spark = SparkSession
        .builder
        .appName("some-procesor-1")
        .config("spark.streaming.kafka.consumer.cache.maxCapacity", 1)
        .config("spark.streaming.kafka.consumer.cache.initialCapacity", 1)
        .getOrCreate()

      implicit val sc = spark.sparkContext
      implicit val ssc = new StreamingContext(sc, Seconds(batchDuration))

      val kafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Seq(inputTopic), Map(
          "bootstrap.servers" -> kafkaServers,
          "auto.offset.reset" -> "earliest",
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "group.id" -> kafkaGroupId
        )))

      val results = processSomeStream(kafkaStream.map { e => e.value() }, incomingFilterPattern)
      writeToKafka(results, producerConfig, outputTopic)

      ssc
    }

    val ssc = createContext()

    ssc.start()
    ssc.awaitTermination()
  }

  def filterSGWStream(kafkaStream: DStream[String], filterPattern: String): DStream[String] = {
    kafkaStream.filter { sgwInput =>
      sgwInput.contains(filterPattern)
    }
  }

  def processSomeStream(kafkaStream: DStream[String], filterPattern: String): DStream[String] = {
    val filteredStream = filterSGWStream(kafkaStream, filterPattern)

    filteredStream.map(jsonIN => {
      strToSGWrawTransformed(jsonIN) match {
        case Success(sgwRow) => sgwRow
        case Failure(f) =>
          LogManager.getRootLogger.error("Error parsing: " + jsonIN, f)
          InputRow()
      }
    }
    ).filter(sgwRow => sgwRow.valid()).map(validSgwRow => createJson(validSgwRow))
  }

  def strToSGWrawTransformed(str: String): Try[InputRow] = {
    val json = Utilities.parseIncomingJSON(str)
    Try(InputRow(json._1, json._2, json._3))
  }

  def writeToKafka(stream: DStream[String], props: Properties, topic: String): Unit = {
    stream.writeToKafka(props, { raw =>
      new ProducerRecord[String, String](
        topic, raw
      )
    })
  }

  def createJson(sgwLogRow: InputRow): String = {
    case class OutputObject(requestDateTime: Long, subscriberID: String, downstreamSystem: String, requestUrl: String, latency: Long, serverNumber: Int)
    val instance = OutputObject(sgwLogRow.requestDateTime, sgwLogRow.subscriberID, sgwLogRow.downstreamSystem, sgwLogRow.requestUrl, sgwLogRow.latency, sgwLogRow.serverNumber)
    instance.asJson.noSpaces
  }
}
