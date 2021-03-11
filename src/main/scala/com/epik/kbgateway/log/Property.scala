package com.epik.kbgateway.log

import java.nio.{ByteBuffer, ByteOrder}
import java.sql.Timestamp

/**
  * Property, could be bind to a Vertex or an Edge.
  *
  * @param name            property's name
  * @param `type`          property's type, but must be among supported types.
  * @param defaultValueOpt default value Or actual value. nullable.
  */
case class Property(name: String, `type`: PropertyType.Value, defaultValueOpt: Option[Any]) {
  require(name.nonEmpty, "Property name can't be empty")

  lazy val nameUtf8Bytes = name.getBytes("UTF-8")
  lazy val totalLength: Int =
    4 /*name size*/ + nameUtf8Bytes.size + 1 /*type size*/ + 1 /*whether have default value*/ + defaultValueSize


  val getBytes: ByteBuffer = {
    val byteBuffer = ByteBuffer.allocate(totalLength).order(ByteOrder.BIG_ENDIAN)
    byteBuffer
      .putInt(nameUtf8Bytes.size) // 4 byte int
      .put(nameUtf8Bytes)
      .put(`type`.id.toByte) // 1 byte type
      .put(defaultValueOpt.fold(0x00.toByte)(_ => 0x01.toByte)) // 1 byte flags to indicate whether have default value.

    (defaultValueOpt: @unchecked) match {
      case Some(v: Int) => byteBuffer.putInt(v)
      case Some(v: Double) => byteBuffer.putDouble(v)
      case Some(v: String) => {
        byteBuffer.putInt(v.getBytes("UTF-8").size)
        byteBuffer.put(v.getBytes("UTF-8"))
      }
      case Some(v: Timestamp) => byteBuffer.putLong(v.getTime)
      case None => {}
    }

    byteBuffer.rewind()
    byteBuffer

  }

  /**
    * byte	1 byte
    * short	2 bytes
    * int	4 bytes
    * long	8 bytes
    * float	4 bytes
    * double	8 bytes
    * boolean	1 bit
    * char	2 bytes
    */
  def defaultValueSize: Int = (defaultValueOpt: @unchecked) match {
    case Some(_: Int) => 4
    case Some(_: Long) => 8
    case Some(_: Double) => 8
    case Some(v: String) =>
      4 + v.getBytes("UTF-8").size // 4=int size, default value size when its is of type string
    case Some(_: Timestamp) => 8 // as Long
    case None => 0
  }

  /**
    * wrap string type into quot, used in gsql format.
    */
  def wrappedDefaultValue: Option[Any] = defaultValueOpt.map {
    case v: String => raw""""${v}""""
    case v => v
  }

}

object Property {

  /**
    * Factory method to parse a Property from binary.
    */
  def apply(binary: ByteBuffer): Property = {
    val nameLength = binary.getInt
    assert(nameLength > 0, s"nameLength ${nameLength} <= 0 when parse Property.")
    val nameBytes = new Array[Byte](nameLength)
    binary.get(nameBytes)

    val bt = binary.get()
    assert(bt >= PropertyType.INTEGER.id && bt <= PropertyType.TIMESTAMP.id)

    val isHasDefaultValue = binary.get()

    val defaultValue = if (isHasDefaultValue == 0x01) {
      PropertyType(bt) match {
        case PropertyType.INTEGER => Some(binary.getInt())
        case PropertyType.DOUBLE => Some(binary.getDouble())
        case PropertyType.STRING => {
          val defaultStrBytes = new Array[Byte](binary.getInt())
          binary.get(defaultStrBytes)
          Some(new String(defaultStrBytes, "UTF-8"))
        }
        case PropertyType.TIMESTAMP => Some(new Timestamp(binary.getLong()))
      }
    } else {
      None
    }

    Property(new String(nameBytes, "UTF-8"), PropertyType(bt.toInt), defaultValue)
  }

}
