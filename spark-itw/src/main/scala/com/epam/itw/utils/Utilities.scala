package com.epam.itw.utils

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}

import io.circe.Json
import io.circe.parser._

import scala.util.matching.Regex

object Utilities {

  def parseTime8(time: String, pattern: String): Long = {
    val formatter = DateTimeFormatter.ofPattern(pattern)
    val zonedDateTime = ZonedDateTime.parse(time, formatter)
    Instant.from(zonedDateTime).toEpochMilli
  }

  def parseTime8toUTC(time: String, pattern: String): Long = {
    val formatter = DateTimeFormatter.ofPattern(pattern)
    val temporalAccessor = formatter.parse(time)
    val localDateTime = LocalDateTime.from(temporalAccessor)
    val zonedDateTime = ZonedDateTime.of(localDateTime, ZoneId.of("UTC"))
    Instant.from(zonedDateTime).toEpochMilli
  }

  /**
    *
    * @param json incoming json
    * @return
    */
  def parseIncomingJSON(json: String): (String, String, String) = {
    val parseResult = parse(json).getOrElse(Json.Null)
    val environment = parseResult.hcursor.get[String]("environment").getOrElse(null)
    val message = parseResult.hcursor.get[String]("message").getOrElse(null)
    val source = parseResult.hcursor.get[String]("source").getOrElse(null)
    (environment, message, source)
  }

  def parseNgnixIncomingJSON(json: String): (String, String, String) = {
    val parseResult = parse(json).getOrElse(Json.Null)

    val environment = parseResult.hcursor.downField("fields").get[String]("environment").getOrElse(null)
    val hostname = parseResult.hcursor.downField("beat").get[String]("hostname").getOrElse(null)
    val message = parseResult.hcursor.get[String]("message").getOrElse(null)

    (environment, message, hostname)
  }

  val sourcePattern: Regex = "^.*\\/([^\\/]+)\\/.*".r
  val serverNumberPattern: Regex = "^\\D*([\\d]*)".r

  /**
    * Get server number from log path
    * <p>In following path example /home/upcadmin/logs_logstash/nlauth1/sgw.log.2017-02-01.199.
    * Number 1 from 'nlauth1' last directory will be our server number
    *
    * @param sourceField source field in incoming JSON
    * @return
    */
  def getLogServerNumber(sourceField: String): Int = {
    val sourcePattern(serverName) = sourceField
    val serverNumberPattern(serverNumber) = serverName
    serverNumber.toInt
  }
}
