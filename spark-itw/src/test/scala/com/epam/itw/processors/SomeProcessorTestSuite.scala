package com.epam.itw.processors

import com.holdenkarau.spark.testing.{SharedSparkContext, StreamingSuiteBase}
import com.epam.itw.processors.SomeProcessor._
import com.epam.itw.utils.Utilities._
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.FunSuite

class SomeProcessorTestSuite extends FunSuite with StreamingSuiteBase with SharedSparkContext {

  val incomingJSONV1 = "{\"@timestamp\":\"2017-02-02T15:28:13.461Z\",\"beat\":{\"hostname\":\"nl-ams02d-monaps1\",\"name\":\"nl-ams02d-monaps1\",\"version\":\"5.1.2\"},\"environment\":\"NL\",\"input_type\":\"log\",\"message\":\"2017/02/01 16:27:00.563 [HttpProxyServlet] [qtp745847881-22295]:   INFO: [58920c544e387ddb2632b881] The request from: [08-1795651338] to: vip-nld-nds-cmdc-ext:9090/cmdc/client/1795651338~8104820_nl_0~8104820_nl/content?lang=dut\u0026classification=crid%3A%2F%2Fupc.com%2F95bc2522-125a-49cb-b72f-33807f7af2b2\u0026contentSize=standard\u0026count=6\u0026collapse=false, the aliasConfigID=cmdc, latency:2 miliseconds   \",\"offset\":6919830,\"source\":\"/home/upcadmin/logs_logstash/nlauth1/sgw.log.2017-02-01.199\",\"type\":\"in.hrzdtv.sgw.raw\"}"

  test("Message field, test split") {
    val messageField = "2017/02/01 16:27:00.563 [HttpProxyServlet] [qtp745847881-22295]:   INFO: [58920c544e387ddb2632b881] The request from: [08-1795651338] to: vip-nld-nds-cmdc-ext:9090/cmdc/client/1795651338~8104820_nl_0~8104820_nl/content?lang=dut\u0026classification=crid%3A%2F%2Fupc.com%2F95bc2522-125a-49cb-b72f-33807f7af2b2\u0026contentSize=standard\u0026count=6\u0026collapse=false, the aliasConfigID=cmdc, latency:2 miliseconds   "

    val expected = Array("2017/02/01", "16:27:00.563", "[HttpProxyServlet]", "[qtp745847881-22295]:", "INFO:", "[58920c544e387ddb2632b881]", "The", "request", "from:", "[08-1795651338]", "to:", "vip-nld-nds-cmdc-ext:9090/cmdc/client/1795651338~8104820_nl_0~8104820_nl/content?lang=dut\u0026classification=crid%3A%2F%2Fupc.com%2F95bc2522-125a-49cb-b72f-33807f7af2b2\u0026contentSize=standard\u0026count=6\u0026collapse=false,", "the", "aliasConfigID=cmdc,", "latency:2", "miliseconds")
    val actual = messageField.split(InputRow.continuousSpaceDelimiter.regex)

    println(expected.mkString("|"))
    println(actual.mkString("|"))

    assert(actual.deep == expected.deep)
  }

  test("NEWJSON, parse input. Get 'Environment', 'Message' and 'Source' fields") {
    val incomingLine = incomingJSONV1

    val expected = ("NL", "2017/02/01 16:27:00.563 [HttpProxyServlet] [qtp745847881-22295]:   INFO: [58920c544e387ddb2632b881] The request from: [08-1795651338] to: vip-nld-nds-cmdc-ext:9090/cmdc/client/1795651338~8104820_nl_0~8104820_nl/content?lang=dut&classification=crid%3A%2F%2Fupc.com%2F95bc2522-125a-49cb-b72f-33807f7af2b2&contentSize=standard&count=6&collapse=false, the aliasConfigID=cmdc, latency:2 miliseconds   ", "/home/upcadmin/logs_logstash/nlauth1/sgw.log.2017-02-01.199")
    val actual = parseIncomingJSON(incomingLine)

    println(incomingLine)
    println(expected)
    println(actual)

    assert(actual == expected)
  }

  test("SGWtransformed, test create object from Message string") {
    val actual = InputRow("NL", "2017/02/01 16:27:00.563 [HttpProxyServlet] [qtp745847881-22295]:   INFO: [58920c544e387ddb2632b881] The request from: [08-1795651338] to: vip-nld-nds-cmdc-ext:9090/cmdc/client/1795651338~8104820_nl_0~8104820_nl/content?lang=dut\u0026classification=crid%3A%2F%2Fupc.com%2F95bc2522-125a-49cb-b72f-33807f7af2b2\u0026contentSize=standard\u0026count=6\u0026collapse=false, the aliasConfigID=cmdc, latency:2 miliseconds   ", "/home/upcadmin/logs_logstash/nlauth1/sgw.log.2017-02-01.199")

    val expectedTimestamp = 1485966420563L
    val expectedSubscriberID = "[08-1795651338]"
    val expectedDownstreamSystem = "vip-nld-nds-cmdc-ext"
    val expectedRequestUrl = "/cmdc/client/1795651338~8104820_nl_0~8104820_nl/content?lang=dut&classification=crid%3A%2F%2Fupc.com%2F95bc2522-125a-49cb-b72f-33807f7af2b2&contentSize=standard&count=6&collapse=false,"
    val expectedLatency = 2L
    val expectedServerNumber = 1

//    assert(actual.requestDateTime == expectedTimestamp)
//    assert(actual.subscriberID == expectedSubscriberID)
//    assert(actual.downstreamSystem == expectedDownstreamSystem)
//    assert(actual.requestUrl == expectedRequestUrl)
//    assert(actual.latency == expectedLatency)
//    assert(actual.serverNumber == expectedServerNumber)
  }

  test("Filter incoming, isHttpProxyServletMessage?") {
    val expected = true

    val kafkaJSON = "{\"@timestamp\":\"2017-02-02T15:28:13.461Z\",\"beat\":{\"hostname\":\"nl-ams02d-monaps1\",\"name\":\"nl-ams02d-monaps1\",\"version\":\"5.1.2\"},\"environment\":\"NL\",\"input_type\":\"log\",\"message\":\"2017/02/01 16:27:00.563 [HttpProxyServlet] [qtp745847881-22295]:   INFO: [58920c544e387ddb2632b881] The request from: [08-1795651338] to: vip-nld-nds-cmdc-ext:9090/cmdc/client/1795651338~8104820_nl_0~8104820_nl/content?lang=dut\u0026classification=crid%3A%2F%2Fupc.com%2F95bc2522-125a-49cb-b72f-33807f7af2b2\u0026contentSize=standard\u0026count=6\u0026collapse=false, the aliasConfigID=cmdc, latency:2 miliseconds   \",\"offset\":6919830,\"source\":\"/home/upcadmin/logs_logstash/nlauth1/sgw.log.2017-02-01.199\",\"type\":\"in.hrzdtv.sgw.raw\"}"
    val actual = kafkaJSON.contains("HttpProxyServlet")

    assert(actual == expected)
  }

  def isHttpProxyServletMessage(input: String, pattern: String): Boolean = input.contains(pattern)

  test("Main Flow. Filter incoming stream with pattern") {
    val actual = Seq(
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq("{\"@timestamp\":\"2017-02-02T15:28:13.461Z\",\"beat\":{\"hostname\":\"nl-ams02d-monaps1\",\"name\":\"nl-ams02d-monaps1\",\"version\":\"5.1.2\"},\"environment\":\"NL\",\"input_type\":\"log\",\"message\":\"2017/02/01 16:27:00.563 [HttpProxyServlet] [qtp745847881-22295]:   INFO: [58920c544e387ddb2632b881] The request from: [08-1795651338] to: vip-nld-nds-cmdc-ext:9090/cmdc/client/1795651338~8104820_nl_0~8104820_nl/content?lang=dut\u0026classification=crid%3A%2F%2Fupc.com%2F95bc2522-125a-49cb-b72f-33807f7af2b2\u0026contentSize=standard\u0026count=6\u0026collapse=false, the aliasConfigID=cmdc, latency:2 miliseconds   \",\"offset\":6919830,\"source\":\"/home/upcadmin/logs_logstash/nlauth1/sgw.log.2017-02-01.199\",\"type\":\"in.hrzdtv.sgw.raw\"}"),
      Seq("{\"@timestamp\":\"2017-02-02T15:28:13.461Z\",\"beat\":{\"hostname\":\"nl-ams02d-monaps1\",\"name\":\"nl-ams02d-monaps1\",\"version\":\"5.1.2\"},\"environment\":\"NL\",\"input_type\":\"log\",\"message\":\"2017/02/01 16:27:00.563 [HttpProxasdrvlet] [qtp745847881-22295]:   INFO: [58920c544e387ddb2632b881] The request from: [08-1795651338] to: vip-nld-nds-cmdc-ext:9090/cmdc/client/1795651338~8104820_nl_0~8104820_nl/content?lang=dut\u0026classification=crid%3A%2F%2Fupc.com%2F95bc2522-125a-49cb-b72f-33807f7af2b2\u0026contentSize=standard\u0026count=6\u0026collapse=false, the aliasConfigID=cmdc, latency:2 miliseconds   \",\"offset\":6919830,\"source\":\"/home/upcadmin/logs_logstash/nlauth1/sgw.log.2017-02-01.199\",\"type\":\"in.hrzdtv.sgw.raw\"}"),
      Seq()
    )

    val expected = Seq(
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq("{\"@timestamp\":\"2017-02-02T15:28:13.461Z\",\"beat\":{\"hostname\":\"nl-ams02d-monaps1\",\"name\":\"nl-ams02d-monaps1\",\"version\":\"5.1.2\"},\"environment\":\"NL\",\"input_type\":\"log\",\"message\":\"2017/02/01 16:27:00.563 [HttpProxyServlet] [qtp745847881-22295]:   INFO: [58920c544e387ddb2632b881] The request from: [08-1795651338] to: vip-nld-nds-cmdc-ext:9090/cmdc/client/1795651338~8104820_nl_0~8104820_nl/content?lang=dut\u0026classification=crid%3A%2F%2Fupc.com%2F95bc2522-125a-49cb-b72f-33807f7af2b2\u0026contentSize=standard\u0026count=6\u0026collapse=false, the aliasConfigID=cmdc, latency:2 miliseconds   \",\"offset\":6919830,\"source\":\"/home/upcadmin/logs_logstash/nlauth1/sgw.log.2017-02-01.199\",\"type\":\"in.hrzdtv.sgw.raw\"}"),
      Seq(),
      Seq()
    )

    testOperation(actual, wrapperFilterSGWStream, expected)
  }

  def wrapperFilterSGWStream(kafkaStream: DStream[String]): DStream[String] = {
    val returnObject = filterSGWStream(kafkaStream, "HttpProxyServlet")

    returnObject.print()

    returnObject
  }

  test("Main Flow. Proceed incoming message to output") {
    val actual = Seq(
      Seq("empty"),
      Seq("empty"),
      Seq("empty"),
      Seq("empty"),
      Seq("{\"@timestamp\":\"2017-02-02T15:28:13.461Z\",\"beat\":{\"hostname\":\"nl-ams02d-monaps1\",\"name\":\"nl-ams02d-monaps1\",\"version\":\"5.1.2\"},\"environment\":\"NL\",\"input_type\":\"log\",\"message\":\"2017/02/01 16:27:00.563 [HttpProxyServlet] [qtp745847881-22295]:   INFO: [58920c544e387ddb2632b881] The request from: [08-1795651338] to: vip-nld-nds-cmdc-ext:9090/cmdc/client/1795651338~8104820_nl_0~8104820_nl/content?lang=dut\u0026classification=crid%3A%2F%2Fupc.com%2F95bc2522-125a-49cb-b72f-33807f7af2b2\u0026contentSize=standard\u0026count=6\u0026collapse=false, the aliasConfigID=cmdc, latency:2 miliseconds   \",\"offset\":6919830,\"source\":\"/home/upcadmin/logs_logstash/nlauth1/sgw.log.2017-02-01.199\",\"type\":\"in.hrzdtv.sgw.raw\"}"),
      Seq("empty"),
      Seq("{\"@timestamp\":\"2017-02-02T15:28:13.461Z\",\"beat\":{\"hostname\":\"nl-ams02d-monaps1\",\"name\":\"nl-ams02d-monaps1\",\"version\":\"5.1.2\"},\"environment\":\"NL\",\"input_type\":\"log\",\"message\":\"2017/02/01 16:27:00.563 [SomeServlet] [qtp745847881-22295]:   INFO: [58920c544e387ddb2632b881] The request from: [08-1795651338] to: vip-nld-nds-cmdc-ext:9090/cmdc/client/1795651338~8104820_nl_0~8104820_nl/content?lang=dut\u0026classification=crid%3A%2F%2Fupc.com%2F95bc2522-125a-49cb-b72f-33807f7af2b2\u0026contentSize=standard\u0026count=6\u0026collapse=false, the aliasConfigID=cmdc, latency:2 miliseconds   \",\"offset\":6919830,\"source\":\"/home/upcadmin/logs_logstash/nlauth1/sgw.log.2017-02-01.199\",\"type\":\"in.hrzdtv.sgw.raw\"}"),
      Seq("{\"@timestamp\":\"2017-02-02T16:31:13.461Z\",\"beat\":{\"hostname\":\"nl-ams02d-monaps1\",\"name\":\"nl-ams02d-monaps1\",\"version\":\"5.1.2\"},\"environment\":\"NL\",\"input_type\":\"log\",\"message\":\"2017/02/01 16:30:00.000 [HttpProxyServlet] [qtp745847881-22295]:   INFO: [58920c544e387ddb2632b881] The request from: [08-1795654567] to: vip-nld-nds-cmdc-ext:9090/cmdc/client/1795651338~8104820_nl_0~8104820_nl/content?lang=dut\u0026classification=crid%3A%2F%2Fupc.com%2F95bc2522-125a-49cb-b72f-33807f7af2b2\u0026contentSize=standard\u0026count=6\u0026collapse=false, the aliasConfigID=cmdc, latency:4 miliseconds   \",\"offset\":6919830,\"source\":\"/home/upcadmin/logs_logstash/nlauth1/sgw.log.2017-02-01.199\",\"type\":\"in.hrzdtv.sgw.raw\"}")
    )

    val expected = Seq(
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq("{\"requestDateTime\":1485966420563,\"subscriberID\":\"[08-1795651338]\",\"downstreamSystem\":\"vip-nld-nds-cmdc-ext\",\"requestUrl\":\"/cmdc/client/1795651338~8104820_nl_0~8104820_nl/content?lang=dut&classification=crid%3A%2F%2Fupc.com%2F95bc2522-125a-49cb-b72f-33807f7af2b2&contentSize=standard&count=6&collapse=false,\",\"latency\":2,\"serverNumber\":1}"),
      Seq(),
      Seq(),
      Seq("{\"requestDateTime\":1485966600000,\"subscriberID\":\"[08-1795654567]\",\"downstreamSystem\":\"vip-nld-nds-cmdc-ext\",\"requestUrl\":\"/cmdc/client/1795651338~8104820_nl_0~8104820_nl/content?lang=dut&classification=crid%3A%2F%2Fupc.com%2F95bc2522-125a-49cb-b72f-33807f7af2b2&contentSize=standard&count=6&collapse=false,\",\"latency\":4,\"serverNumber\":1}")
    )

    testOperation(actual, wrapperProcessSGWStream, expected)
  }

  def wrapperProcessSGWStream(kafkaStream: DStream[String]): DStream[String] = {
    val returnObject = processSomeStream(kafkaStream, "HttpProxyServlet")

    returnObject.print()

    returnObject
  }

  test("Main Flow2. Proceed incoming message with ERRORS to output") {
    val actual = Seq(
      Seq("empty"),
      Seq("empty"),
      Seq("empty"),
      Seq("empty"),
      Seq("{\"@timestamp\":\"2017-02-02T15:28:13.461Z\",\"beat\":{\"hostname\":\"nl-ams02d-monaps1\",\"name\":\"nl-ams02d-monaps1\",\"version\":\"5.1.2\"},\"environment\":\"NL\",\"input_type\":\"log\",\"message\":\"2017/32/22 16:27:00.563 [HttpProxyServlet] [qtp745847881-22295]:   INFO: [58920c544e387ddb2632b881] The request from: [08-1795651338] to: vip-nld-nds-cmdc-ext:9090/cmdc/client/1795651338~8104820_nl_0~8104820_nl/content?lang=dut\u0026classification=crid%3A%2F%2Fupc.com%2F95bc2522-125a-49cb-b72f-33807f7af2b2\u0026contentSize=standard\u0026count=6\u0026collapse=false, the aliasConfigID=cmdc, latency:2 miliseconds   \",\"offset\":6919830,\"source\":\"/home/upcadmin/logs_logstash/nlauth1/sgw.log.2017-02-01.199\",\"type\":\"in.hrzdtv.sgw.raw\"}"),
      Seq("empty"),
      Seq("{\"@timestamp\":\"2017-02-02T15:28:13.461Z\",\"beat\":{\"hostname\":\"nl-ams02d-monaps1\",\"name\":\"nl-ams02d-monaps1\",\"version\":\"5.1.2\"},\"environment\":\"NL\",\"input_type\":\"log\",\"message\":\"2017/02/01 16:27:00.563 [SomeServlet] [qtp745847881-22295]:   INFO: [58920c544e387ddb2632b881] The request from: [08-1795651338] to: vip-nld-nds-cmdc-ext:9090/cmdc/client/1795651338~8104820_nl_0~8104820_nl/content?lang=dut\u0026classification=crid%3A%2F%2Fupc.com%2F95bc2522-125a-49cb-b72f-33807f7af2b2\u0026contentSize=standard\u0026count=6\u0026collapse=false, the aliasConfigID=cmdc, latency:2 miliseconds   \",\"offset\":6919830,\"source\":\"/home/upcadmin/logs_logstash/nlauth1/sgw.log.2017-02-01.199\",\"type\":\"in.hrzdtv.sgw.raw\"}"),
      Seq("{\"@timestamp\":\"2017-02-02T16:31:13.461Z\",\"beat\":{\"hostname\":\"nl-ams02d-monaps1\",\"name\":\"nl-ams02d-monaps1\",\"version\":\"5.1.2\"},\"environment\":\"NL\",\"input_type\":\"log\",\"message\":\"2017/02/01 16:30:00.000 [HttpProxyServlet] [qtp745847881-22295]:   INFO: [58920c544e387ddb2632b881] The request from: [08-1795654567] to: vip-nld-nds-cmdc-ext:9090/cmdc/client/1795651338~8104820_nl_0~8104820_nl/content?lang=dut\u0026classification=crid%3A%2F%2Fupc.com%2F95bc2522-125a-49cb-b72f-33807f7af2b2\u0026contentSize=standard\u0026count=6\u0026collapse=false, the aliasConfigID=cmdc, latency:4 miliseconds   \",\"offset\":6919830,\"source\":\"/home/upcadmin/logs_logstash/nlauth1/sgw.log.2017-02-01.199\",\"type\":\"in.hrzdtv.sgw.raw\"}")
    )

    val expected = Seq(
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq("{\"requestDateTime\":1485966600000,\"subscriberID\":\"[08-1795654567]\",\"downstreamSystem\":\"vip-nld-nds-cmdc-ext\",\"requestUrl\":\"/cmdc/client/1795651338~8104820_nl_0~8104820_nl/content?lang=dut&classification=crid%3A%2F%2Fupc.com%2F95bc2522-125a-49cb-b72f-33807f7af2b2&contentSize=standard&count=6&collapse=false,\",\"latency\":4,\"serverNumber\":1}")
    )

    testOperation(actual, wrapperProcessSGWStream, expected)
  }
}
