package com.epik.kbgateway.log

import java.nio.{ByteBuffer, ByteOrder}

/**
  * Tag of Vertex, logically = <name, List[Property]>
  * @param name tag's name
  * @param properties tag's property list
  */
case class Tag(name: String, properties: List[Property]) {
  require(name.nonEmpty, "Tag name can't be empty")
  require(properties.nonEmpty, "Tag's properties can't be empty")

  lazy val nameUtf8Bytes = name.getBytes("UTF-8")

  lazy val totalLength: Int = 4 /*name size*/ + nameUtf8Bytes.size + 4 /*type size*/ + properties
    .foldLeft(0)(_ + _.totalLength)

  val getBytes: ByteBuffer = {
    val byteBuffer = ByteBuffer.allocate(totalLength).order(ByteOrder.BIG_ENDIAN)
    byteBuffer
      .putInt(nameUtf8Bytes.size) // 4 byte int
      .put(nameUtf8Bytes)
      .putInt(properties.size) // 4 byte properties size

    properties.foreach(p => byteBuffer.put(p.getBytes.array()))

    byteBuffer.rewind()
    byteBuffer
  }

}

object Tag {

  /**
    * Factory method to parse a Tag from binary.
    */
  def apply(binary: ByteBuffer): Tag = {
    val nameLength = binary.getInt
    assert(nameLength > 0, "nameLength <0 when parsing Tag.")
    val nameBytes = new Array[Byte](nameLength)
    binary.get(nameBytes)

    val propertiesSize = binary.getInt
    assert(propertiesSize > 0, "propertiesSize <0 when parsing Tag.")
    val propertyList = (0 until propertiesSize).map(_ => Property.apply(binary)).toList

    Tag(new String(nameBytes, "UTF-8"), propertyList)
  }

}
