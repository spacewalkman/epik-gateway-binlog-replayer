package com.epik.kbgateway

import java.io.File
import java.nio.{ByteBuffer, ByteOrder}

import com.epik.kbgateway.log._
import org.apache.commons.io.FileUtils
import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.collection.mutable
import scala.util.Random

class LogTest extends FlatSpec with BeforeAndAfter {

  val properties =
    List(
      Property("some_string_property",
               PropertyType.STRING,
               Some(Random.alphanumeric.take(1000).mkString)),
      Property("some_int_property", PropertyType.INTEGER, Some(Random.nextInt(10000))),
      Property("some_double_property", PropertyType.DOUBLE, Some(Random.nextDouble()))
    )

  val tag = Tag("some_tag", properties)

  "A SchemaGraphDbLog" should "be serialize/deserialized" in {
    Seq(CrudMode.ADD, CrudMode.DELETE).foreach { mode =>
      val log             = SchemaGraphDbLog("some_graph_db_name", 1024, 3, VidGeneratorType.HASH, mode)
      val binary          = log.getBytes
      val logDeserialized = SchemaGraphDbLog.apply(binary)

      assert(
        log == logDeserialized
      )
    }
  }

  "A SchemaTagLog" should "be serialize/deserialized" in {
    val log             = SchemaTagLog("some_graph_db_name", "vertex_tag", properties)
    val binary          = log.getBytes
    val logDeserialized = SchemaTagLog.apply(binary)

    assert(
      log == logDeserialized
    )
  }

  "A SchemaEdgeLog" should "be serialize/deserialized" in {
    val log             = SchemaEdgeLog("some_graph_db_name", "some_edge_name", properties)
    val binary          = log.getBytes
    val logDeserialized = SchemaEdgeLog.apply(binary)

    assert(
      log == logDeserialized
    )
  }

  "A DataVertexLog" should "be serialize/deserialized" in {
    val log             = DataVertexLog("some_graph_db_name", "vertex_business_id", List(tag))
    val binary          = log.getBytes
    val logDeserialized = DataVertexLog.apply(binary)

    assert(
      log == logDeserialized
    )
  }

  "A DataEdgeLog" should "be serialize/deserialized" in {
    val log = DataEdgeLog("some_graph_db_name_中国汉字",
                          "some_edge_name_中国汉字",
                          "from_vertex_business_id_中国汉字",
                          "to_vertex_business_id_中国汉字",
                          properties)

    val binary          = log.getBytes
    val logDeserialized = DataEdgeLog.apply(binary)

    assert(
      log == logDeserialized
    )
  }

  "A GetBytes and GetUTF8_bytes" should "be serialize/deserialized" in {
    val size1 = "some_graph_db_name_中国汉字".getBytes("UTF-8").size
    val size2 = "some_graph_db_name_中国汉字".size
    val size3 = "中国汉字".getBytes("UTF-8").size
    val size4 = "中国汉字".size
    println(s"UTF-8.size()=${size1} , size()=${size2}, size3=${size3},size4=${size4}")
    //assert(size1 == size2)
  }

  val logs = List(
    SchemaGraphDbLog("some_graph_db_name_中国汉字", 1024, 3),
    SchemaGraphDbLog("some_graph_db_name_中国汉字", 1024, 3, VidGeneratorType.HASH, CrudMode.DELETE),
    SchemaTagLog("some_graph_db_name_中国汉字", "vertex_tag_中国汉字", properties),
    SchemaEdgeLog("some_graph_db_name_中国汉字", "some_edge_name_中国汉字", properties),
    DataVertexLog("some_graph_db_name_中国汉字", "vertex_business_id_中国汉字", List(tag)),
    DataEdgeLog("some_graph_db_name_中国汉字",
                "some_edge_name_中国汉字",
                "from_vertex_business_id_中国汉字",
                "to_vertex_business_id_中国汉字",
                properties),
    DataEdgeLog("some_graph_db_name_中国汉字",
                "some_edge_name_中国汉字",
                "from_vertex_business_id_中国汉字",
                "to_vertex_business_id_中国汉字",
                properties)
  )

  "A LogList" should "be serialized/deserialized using fold" in {
    // the following 2 style should be identical.

    //fold, functional-style
    val binary: Array[Byte] = logs.foldLeft(Array[Byte]())(_ ++ _.getBytes.array())

    // imperative-style
    val totalLogLength = logs.foldLeft(0)(_ + _.totalLength)
    val buffer         = ByteBuffer.allocate(totalLogLength).order(ByteOrder.BIG_ENDIAN)
    logs.foreach(log => buffer.put(log.getBytes.array()))

    binary shouldBe buffer.array()
  }

  "Logs" should "be parsed by Factory method of object Log" in {
    val totalLogLength = logs.foldLeft(0)(_ + _.totalLength)
    val buffer         = ByteBuffer.allocate(totalLogLength).order(ByteOrder.BIG_ENDIAN)
    logs.foreach(log => buffer.put(log.getBytes.array()))

    // Log.Parse factory method
    buffer.rewind()
    var logBufferArray = mutable.ArrayBuffer[Log]()
    while (buffer.hasRemaining) {
      val log = Log.parse(buffer)
      logBufferArray += log
    }

    logs shouldBe logBufferArray.toList
  }

  "A LogList" should "be save to and read from a binary file" in {
    val tempFile = File.createTempFile("epik-log", null)
    tempFile.deleteOnExit()

    val totalLogLength      = logs.foldLeft(0)(_ + _.totalLength)
    val binary: Array[Byte] = logs.foldLeft(Array[Byte]())(_ ++ _.getBytes.array())

    FileUtils.writeByteArrayToFile(tempFile, binary)
    FileUtils.sizeOf(tempFile) shouldBe totalLogLength

    val readBackContentBytes = FileUtils.readFileToByteArray(tempFile)
    readBackContentBytes.size shouldBe totalLogLength
    readBackContentBytes shouldBe binary
  }

  //TODO: endianess problem
  "A LogList" should "be parsed from a spark generated file" in {
    val bytes = Array[Byte](25, 0, 0, 0, 10, 99, 110, 95, 100, 98, 112, 101, 100, 105, 97, 0, 0, 0,
      2, 33, 61, 0, 0, 0, 0, 1, 0, 0, 0, 13, 100, 111, 109, 97, 105, 110, 95, 101, 110, 116, 105,
      116, 121, 0, 0, 0, 1, 0, 0, 0, 16, 100, 111, 109, 97, 105, 110, 95, 112, 114, 101, 100, 105,
      99, 97, 116, 101, 2, 1, 0, 0, 0, 2, 33, 61, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    var logBufferArray = mutable.ArrayBuffer[Log]()
    val buffer         = ByteBuffer.wrap(bytes)
    while (buffer.hasRemaining) {
      val log = Log.parse(buffer)
      println(log)
      logBufferArray += log
    }
  }

  ignore should "be deserialized from binary files generated by spark" in {
    val binaryFileContent: Array[Byte] =
      FileUtils.readFileToByteArray(new File("/Users/qianyong1/vm/data/first2.log"))
    val buffer = ByteBuffer.wrap(binaryFileContent)

    var logBufferArray = mutable.ArrayBuffer[Log]()
    while (buffer.hasRemaining) {
      val log = Log.parse(buffer)
      println(log)
      logBufferArray += log
    }

  }
}
