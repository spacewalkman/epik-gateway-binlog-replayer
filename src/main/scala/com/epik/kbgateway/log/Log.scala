package com.epik.kbgateway.log

import java.nio.{ByteBuffer, ByteOrder}

/**
  * Log header: bitmap mode
  */
object SchemaDataMode extends Enumeration {
  val SCHEMA = Value(0x00.toByte)
  val DATA   = Value(0x01.toByte)
}

/**
  * Graphdb/Tag/Edge
  */
object DbTagEdgeMode extends Enumeration {
  val GRAPHDB = Value(0x01.toByte)
  val TAG     = Value(0x02.toByte)
  val EDGE    = Value(0x03.toByte)
}

/**
  * CRUD mode, exclude Read(R)
  */
object CrudMode extends Enumeration {
  val ADD    = Value(0x01)
  val UPDATE = Value(0x02)
  val DELETE = Value(0x03)
}

case class BitMode(schemaDataMode: SchemaDataMode.Value,
                   dbTagEdgeMode: DbTagEdgeMode.Value,
                   crudMode: CrudMode.Value = CrudMode.ADD) {
  val getByte: Byte =
    (schemaDataMode.id.toByte << 4 | dbTagEdgeMode.id.toByte << 2 | crudMode.id.toByte).toByte
  override def toString: String = getByte.toString
}

object BitMode {
  def apply(binary: Byte): BitMode = {
    val schemaData = (binary >> 4) & 0x01
    val dbTagEdge  = (binary >> 2) & 0x03
    val crud       = binary & 0x03

    BitMode(SchemaDataMode(schemaData), DbTagEdgeMode(dbTagEdge), CrudMode(crud))
  }
}

/**
  * Base class of all Log type.
  */
sealed abstract class Log {

  val getBitMode: BitMode

  /**
    * total length = header(bitmap mode length, which is 1) + payloadLength
    */
  def totalLength: Int = 1 + payLoadLength

  def getBytes: ByteBuffer

  def payLoadLength: Int
}

/**
  * Property Type,  Currently only support 4 types: Integer、Double、String、Timestamp
  */
object PropertyType extends Enumeration {
  val INTEGER   = Value("int")
  val DOUBLE    = Value("double")
  val STRING    = Value("string")
  val TIMESTAMP = Value("timestamp")
}

object Log {
  implicit object LogOrdering extends Ordering[Log] {
    override def compare(p1: Log, p2: Log): Int = {
      (p1, p2) match {
        case (_: SchemaGraphDbLog, _: SchemaGraphDbLog) =>
          0
        case (_: SchemaGraphDbLog, _)             => 1
        case (_, _: SchemaGraphDbLog)             => -1
        case (_: SchemaTagLog, _)                 => 1
        case (_: SchemaEdgeLog, _)                => 1
        case (_: DataVertexLog, _: DataEdgeLog)   => 1 // make all vertex greater than edge.
        case (_: DataEdgeLog, _: DataVertexLog)   => -1
        case (a: DataVertexLog, b: DataVertexLog) => a.vertexBizId.compareTo(b.vertexBizId)
        case (a: DataEdgeLog, b: DataEdgeLog)     => a.fromVertexBizId.compareTo(b.fromVertexBizId)
        case _                                    => throw new IllegalStateException("Should not be here.")
      }
    }
  }

  def parse(byteBuffer: ByteBuffer): Log = {
    BitMode.apply(byteBuffer.get()) match {
      case BitMode(_ @SchemaDataMode.SCHEMA, _ @DbTagEdgeMode.GRAPHDB, _) => {
        byteBuffer.position(byteBuffer.position - 1)
        SchemaGraphDbLog.apply(byteBuffer)
      }
      case BitMode(_ @SchemaDataMode.SCHEMA, _ @DbTagEdgeMode.TAG, _) => {
        byteBuffer.position(byteBuffer.position - 1)
        SchemaTagLog.apply(byteBuffer)
      }
      case BitMode(_ @SchemaDataMode.SCHEMA, _ @DbTagEdgeMode.EDGE, _) => {
        byteBuffer.position(byteBuffer.position - 1)
        SchemaEdgeLog.apply(byteBuffer)
      }
      case BitMode(_ @SchemaDataMode.DATA, _ @DbTagEdgeMode.TAG, _) => {
        byteBuffer.position(byteBuffer.position - 1)
        DataVertexLog.apply(byteBuffer)
      }
      case BitMode(_ @SchemaDataMode.DATA, _ @DbTagEdgeMode.EDGE, _) => {
        byteBuffer.position(byteBuffer.position - 1)
        DataEdgeLog.apply(byteBuffer)
      }
      case _ => throw new IllegalStateException("BitMode parse error.")
    }
  }
}

/**
  * Schema GraphDB Log
  *
  * @param graphDbName graph database name
  * @param partition partition size
  * @param replicas replica size of a partition
  * @param crudMode maybe ADD/UPDATE/DELETE
  */
case class SchemaGraphDbLog(graphDbName: String,
                            partition: Int,
                            replicas: Short,
                            vidGeneratorType: VidGeneratorType.Value = VidGeneratorType.HASH,
                            crudMode: CrudMode.Value = CrudMode.ADD)
    extends Log {
  require(graphDbName.nonEmpty)
  require(partition > 0)
  require(replicas > 0)

  lazy val graphDbNameUtf8Bytes = graphDbName.getBytes("UTF-8")

  override val getBitMode: BitMode =
    BitMode(SchemaDataMode.SCHEMA, DbTagEdgeMode.GRAPHDB, crudMode)

  override val payLoadLength
    : Int = 4 /*graphDbName size*/ + graphDbNameUtf8Bytes.size + 4 /*partition*/ + 2 /*replicas*/ + 1 /*VID generator*/

  override val getBytes: ByteBuffer = {
    val byteBuffer = ByteBuffer
      .allocate(totalLength)
      .order(ByteOrder.BIG_ENDIAN)
      .put(getBitMode.getByte)
      .putInt(graphDbNameUtf8Bytes.size)
      .put(graphDbNameUtf8Bytes)
      .putInt(partition)
      .putShort(replicas)
      .put(vidGeneratorType.id.toByte)

    byteBuffer.rewind()
    byteBuffer
  }
}

object SchemaGraphDbLog {

  /**
    * Factory method to deserialize SchemaGraphDbLog from binary
    */
  def apply(binary: ByteBuffer): SchemaGraphDbLog = {
    val bitMod = BitMode.apply(binary.get())
    assert(bitMod.schemaDataMode == SchemaDataMode.SCHEMA)
    assert(bitMod.dbTagEdgeMode == DbTagEdgeMode.GRAPHDB)
    assert(bitMod.crudMode == CrudMode.ADD || bitMod.crudMode == CrudMode.DELETE)

    val graphDbSize = binary.getInt
    assert(graphDbSize > 0, "graphDbSize < 0 when parse SchemaGraphDbLog.")
    val graphDbNameBytes = new Array[Byte](graphDbSize)
    binary.get(graphDbNameBytes)
    val graphDbNameStr = new String(graphDbNameBytes, "UTF-8")

    val partitionNum = binary.getInt
    assert(partitionNum > 0, "partition < 0 when parse SchemaGraphDbLog.")

    val replicaNum = binary.getShort
    assert(replicaNum > 0, "replicas < 0 when parse SchemaGraphDbLog.")

    val vidGenerator = VidGeneratorType(binary.get())
    assert(vidGenerator == VidGeneratorType.HASH, "only Hash VID generator supported.")

    new SchemaGraphDbLog(graphDbNameStr, partitionNum, replicaNum, vidGenerator, bitMod.crudMode)
  }
}

/**
  * Schema Tag
  */
case class SchemaTagLog(graphDbName: String,
                        tagName: String,
                        properties: List[Property],
                        crudMode: CrudMode.Value = CrudMode.ADD)
    extends Log {
  require(graphDbName.nonEmpty)
  require(tagName.nonEmpty)
  require(properties.nonEmpty)

  override lazy val payLoadLength: Int =
    4 + graphDbNameUtf8Bytes.size + 4 + tagNameUtf8Bytes.size + 4 /*properties size*/ + properties
      .foldLeft(0)(_ + _.totalLength)
  lazy val graphDbNameUtf8Bytes = graphDbName.getBytes("UTF-8")
  lazy val tagNameUtf8Bytes     = tagName.getBytes("UTF-8")

  override val getBitMode: BitMode =
    BitMode(SchemaDataMode.SCHEMA, DbTagEdgeMode.TAG, crudMode)

  override val getBytes: ByteBuffer = {
    val byteBuffer = ByteBuffer.allocate(totalLength).order(ByteOrder.BIG_ENDIAN)
    //Every log need put Bitmap mode first.
    byteBuffer.put(getBitMode.getByte)
    byteBuffer.putInt(graphDbNameUtf8Bytes.size)
    byteBuffer.put(graphDbNameUtf8Bytes)
    byteBuffer.putInt(tagNameUtf8Bytes.size)
    byteBuffer.put(tagNameUtf8Bytes)
    byteBuffer.putInt(properties.size)

    assert(
      (1 + 4 + graphDbNameUtf8Bytes.size + 4 + tagNameUtf8Bytes.size + 4) == byteBuffer.position())
    assert(properties.size > 0)
    properties.foreach(p => {
      byteBuffer
        .put(p.getBytes.array()) //CAUTIONS: must call array to materialized Property's ByteBuffer's content.
    })

    byteBuffer.rewind()
    byteBuffer
  }
}

object SchemaTagLog {

  /**
    * Factory method to deserialize SchemaTagLog from binary
    */
  def apply(binary: ByteBuffer): SchemaTagLog = {
    val startPosition = binary.position()
    val bitMod        = BitMode.apply(binary.get())
    assert(bitMod.schemaDataMode == SchemaDataMode.SCHEMA,
           s"${bitMod.schemaDataMode}!=SchemaDataMode.SCHEMA")
    assert(bitMod.dbTagEdgeMode == DbTagEdgeMode.TAG)

    val graphDbSize = binary.getInt
    assert(graphDbSize > 0, "graphDbSize < 0 when parse SchemaTagLog.")
    val graphDbNameBytes = new Array[Byte](graphDbSize)
    binary.get(graphDbNameBytes)
    val graphDbNameStr = new String(graphDbNameBytes, "UTF-8")

    val tagNameSize = binary.getInt
    assert(tagNameSize > 0, "tagNameSize < 0 when parse SchemaTagLog.")
    val tagNameBytes = new Array[Byte](tagNameSize)
    binary.get(tagNameBytes)
    val tagNameString = new String(tagNameBytes, "UTF-8")

    val propertyCount = binary.getInt
    assert(propertyCount > 0, "propertyCount < 0 when parse SchemaTagLog.")
    assert((1 + 4 + graphDbSize + 4 + tagNameSize + 4) == (binary.position() - startPosition))
    val propertyList = (0 until propertyCount).map(_ => Property(binary)).toList

    new SchemaTagLog(graphDbNameStr, tagNameString, propertyList, bitMod.crudMode)
  }

}

/**
  * Schema Edge Log, almost same as Schema Tag
  */
case class SchemaEdgeLog(graphDbName: String,
                         edgeName: String,
                         properties: List[Property],
                         crudMode: CrudMode.Value = CrudMode.ADD)
    extends Log {
  require(graphDbName.nonEmpty)
  require(edgeName.nonEmpty)
  require(properties.nonEmpty)

  lazy val graphDbNameUtf8Bytes = graphDbName.getBytes("UTF-8")
  lazy val edgeNameUtf8Bytes    = edgeName.getBytes("UTF-8")

  override lazy val getBitMode: BitMode =
    BitMode(SchemaDataMode.SCHEMA, DbTagEdgeMode.EDGE, crudMode)

  override val payLoadLength: Int =
    4 + graphDbNameUtf8Bytes.size + 4 + edgeNameUtf8Bytes.size + 4 /*properties size*/ + properties
      .foldLeft(0)(
        _ + _.totalLength
      )

  override def getBytes: ByteBuffer = {
    val byteBuffer = ByteBuffer.allocate(totalLength).order(ByteOrder.BIG_ENDIAN)
    byteBuffer.put(getBitMode.getByte)
    byteBuffer.putInt(graphDbNameUtf8Bytes.size)
    byteBuffer.put(graphDbNameUtf8Bytes)
    byteBuffer.putInt(edgeNameUtf8Bytes.size)
    byteBuffer.put(edgeNameUtf8Bytes)
    byteBuffer.putInt(properties.size)
    properties.foreach(p => byteBuffer.put(p.getBytes.array()))

    byteBuffer.rewind()
    byteBuffer
  }

}

object SchemaEdgeLog {

  def apply(binary: ByteBuffer): SchemaEdgeLog = {
    val bitMod = BitMode.apply(binary.get())
    assert(bitMod.schemaDataMode == SchemaDataMode.SCHEMA)
    assert(bitMod.dbTagEdgeMode == DbTagEdgeMode.EDGE)

    val graphDbSize = binary.getInt
    assert(graphDbSize > 0, "graphDbSize < 0 when parse SchemaEdgeLog.")
    val graphDbNameBytes = new Array[Byte](graphDbSize)
    binary.get(graphDbNameBytes)
    val graphDbNameStr = new String(graphDbNameBytes, "UTF-8")

    val edgeNameSize = binary.getInt
    assert(edgeNameSize > 0, "edgeNameSize < 0 when parse SchemaEdgeLog.")
    val edgeNameBytes = new Array[Byte](edgeNameSize)
    binary.get(edgeNameBytes)
    val edgeNameStr = new String(edgeNameBytes, "UTF-8")

    val propertiesSize = binary.getInt
    assert(propertiesSize > 0, "propertiesSize < 0 when parse SchemaEdgeLog.")

    SchemaEdgeLog(graphDbNameStr,
                  edgeNameStr,
                  (0 until propertiesSize).map(_ => Property.apply(binary)).toList,
                  bitMod.crudMode)
  }
}

/**
  * Vertex ID geneator type.
  */
object VidGeneratorType extends Enumeration {

  /**
    * Hash, corresponding to vid_type = INT64, Currently is the only supported vid generator,
    */
  val HASH = Value(0.toByte)

  /**
    * Fixed length string vid generator
    * TODO: reserved
    */
  val FIX_LENGTH_STRING = Value(1.toByte)
}

/**
  * Data Vertex's Log
  * @param graphDbName graph database name
  * @param vertexBizId Vertex domain value, used by `vidGenerator` to generate VID
  * @param vidGenerator VertexID generator
  * @param tags Tags of Vertex
  */
case class DataVertexLog(graphDbName: String,
                         vertexBizId: String,
                         tags: List[Tag],
                         vidGenerator: VidGeneratorType.Value = VidGeneratorType.HASH,
                         crudMode: CrudMode.Value = CrudMode.ADD)
    extends Log {
  require(graphDbName.nonEmpty)
  //TODO:ava.lang.IllegalArgumentException: requirement failed
  //	at scala.Predef$.require(Predef.scala:268)
  //	at com.epik.kbgateway.log.DataVertexLog.(Log.scala:372)
  require(vertexBizId.nonEmpty)
  require(tags.nonEmpty)

  lazy val graphDbNameUtf8Bytes =
    graphDbName.getBytes("UTF-8")
  lazy val vertexBizIdUtf8Bytes =
    vertexBizId.getBytes("UTF-8")

  override lazy val payLoadLength: Int =
    4 + graphDbNameUtf8Bytes.size + 4 /*vertexBizId size*/ + vertexBizIdUtf8Bytes.size + 1 /*VID generator type*/ + 4 /*tag size*/ + tags
      .foldLeft(0)(
        _ + _.totalLength
      )
  override val getBitMode: BitMode =
    BitMode(SchemaDataMode.DATA, DbTagEdgeMode.TAG, crudMode)

  override def getBytes: ByteBuffer = {
    val byteBuffer = ByteBuffer.allocate(totalLength).order(ByteOrder.BIG_ENDIAN)
    byteBuffer.put(getBitMode.getByte)
    byteBuffer.putInt(graphDbNameUtf8Bytes.size)
    byteBuffer.put(graphDbNameUtf8Bytes)
    byteBuffer.putInt(vertexBizIdUtf8Bytes.size)
    byteBuffer.put(vertexBizIdUtf8Bytes)
    byteBuffer.put(vidGenerator.id.toByte)
    byteBuffer.putInt(tags.size)
    tags.foreach(t => byteBuffer.put(t.getBytes.array()))

    byteBuffer.rewind()
    byteBuffer
  }
}

object DataVertexLog {

  def apply(binary: ByteBuffer): DataVertexLog = {
    val bitMod = BitMode.apply(binary.get())
    assert(bitMod.schemaDataMode == SchemaDataMode.DATA)
    assert(bitMod.dbTagEdgeMode == DbTagEdgeMode.TAG)

    val graphDbSize = binary.getInt
    assert(graphDbSize > 0, "graphDbSize < 0 when parse DataVertexLog.")
    val graphDbNameBytes = new Array[Byte](graphDbSize)
    binary.get(graphDbNameBytes)
    val graphDbNameStr = new String(graphDbNameBytes, "UTF-8")

    val vertexBizIdSize = binary.getInt
    assert(vertexBizIdSize > 0, "vertexBizId Size < 0 when parse DataVertexLog.")
    val vertexBizIdBytes = new Array[Byte](vertexBizIdSize)
    binary.get(vertexBizIdBytes)
    val vertexBizId = new String(vertexBizIdBytes, "UTF-8")

    val vidGenerator = VidGeneratorType(binary.get())
    //TODO: currently only support Hash VID scheme
    assert(vidGenerator == VidGeneratorType.HASH, "only Hash VID generator supported.")

    val tagSize = binary.getInt
    assert(tagSize > 0, "tagSize < 0 when parse DataVertexLog.")

    DataVertexLog(graphDbNameStr,
                  vertexBizId,
                  (0 until tagSize).map(_ => Tag.apply(binary)).toList,
                  vidGenerator,
                  bitMod.crudMode)
  }
}

case class DataEdgeLog(graphDbName: String,
                       edgeName: String,
                       fromVertexBizId: String,
                       toVertexBizId: String,
                       properties: List[Property],
                       vidGenerator: VidGeneratorType.Value = VidGeneratorType.HASH,
                       crudMode: CrudMode.Value = CrudMode.ADD)
    extends Log {
  require(graphDbName.nonEmpty)
  require(edgeName.nonEmpty)
  require(fromVertexBizId.nonEmpty)
  require(toVertexBizId.nonEmpty)
  require(properties.nonEmpty)

  lazy val graphDbNameUtf8Bytes = graphDbName.getBytes("UTF-8")
  lazy val edgeNameUtf8Bytes    = edgeName.getBytes("UTF-8")
  lazy val fromVertexBizIdUtf8Bytes =
    fromVertexBizId.getBytes("UTF-8")
  lazy val toVertexBizIdUtf8Bytes =
    toVertexBizId.getBytes("UTF-8")
  override lazy val payLoadLength: Int =
    4 + graphDbNameUtf8Bytes.size + 4 /*edgeName size*/ + edgeNameUtf8Bytes.size + 4 /*fromVertexBizId size*/ + fromVertexBizIdUtf8Bytes.size + 4 /*toVertexBizId size*/ + toVertexBizIdUtf8Bytes.size + 1 /*VID generator type*/ + 4 /*properties size*/ + properties
      .foldLeft(0)(
        _ + _.totalLength
      )
  override val getBitMode: BitMode =
    BitMode(SchemaDataMode.DATA, DbTagEdgeMode.EDGE, crudMode)

  override def getBytes: ByteBuffer = {
    val byteBuffer = ByteBuffer.allocate(totalLength).order(ByteOrder.BIG_ENDIAN)
    byteBuffer.put(getBitMode.getByte)
    byteBuffer.putInt(graphDbNameUtf8Bytes.size)
    byteBuffer.put(graphDbNameUtf8Bytes)
    byteBuffer.putInt(edgeNameUtf8Bytes.size)
    byteBuffer.put(edgeNameUtf8Bytes)
    byteBuffer.putInt(fromVertexBizIdUtf8Bytes.size)
    byteBuffer.put(fromVertexBizIdUtf8Bytes)
    byteBuffer.putInt(toVertexBizIdUtf8Bytes.size)
    byteBuffer.put(toVertexBizIdUtf8Bytes)
    byteBuffer.put(vidGenerator.id.toByte)
    byteBuffer.putInt(properties.size)
    properties.foreach(p => byteBuffer.put(p.getBytes.array()))

    byteBuffer.rewind()
    byteBuffer
  }

}

object DataEdgeLog {

  def apply(binary: ByteBuffer): DataEdgeLog = {
    val bitMod = BitMode.apply(binary.get())
    assert(bitMod.schemaDataMode == SchemaDataMode.DATA)
    assert(bitMod.dbTagEdgeMode == DbTagEdgeMode.EDGE)

    val graphDbSize = binary.getInt
    assert(graphDbSize > 0, "graphDbSize < 0 when parse DataEdgeLog.")
    val graphDbNameBytes = new Array[Byte](graphDbSize)
    binary.get(graphDbNameBytes)
    val graphDbNameStr = new String(graphDbNameBytes, "UTF-8")

    val edgeNameSize = binary.getInt
    assert(edgeNameSize > 0, "edgeNameSize < 0 when parse DataEdgeLog.")
    val edgeNameBytes = new Array[Byte](edgeNameSize)
    binary.get(edgeNameBytes)
    val edgeNameStr = new String(edgeNameBytes, "UTF-8")

    val fromVertexBizIdSize = binary.getInt
    assert(fromVertexBizIdSize > 0, "fromVertexBizIdSize < 0 when parse DataEdgeLog.")
    val fromVertexBizIdBytes = new Array[Byte](fromVertexBizIdSize)
    binary.get(fromVertexBizIdBytes)
    val fromVertexBizId = new String(fromVertexBizIdBytes, "UTF-8")

    val toVertexBizIdSize = binary.getInt
    assert(toVertexBizIdSize > 0, "toVertexBizIdSize < 0 when parse DataEdgeLog.")
    val toVertexBizIdBytes = new Array[Byte](toVertexBizIdSize)
    binary.get(toVertexBizIdBytes)
    val toVertexBizId = new String(toVertexBizIdBytes, "UTF-8")

    val vidGenerator = VidGeneratorType(binary.get())
    //TODO: currently only support Hash VID scheme
    assert(vidGenerator == VidGeneratorType.HASH, "only Hash VID generator supported.")

    val propertySize = binary.getInt
    assert(propertySize > 0, "propertySize < 0 when parse DataEdgeLog.")

    DataEdgeLog(graphDbNameStr,
                edgeNameStr,
                fromVertexBizId,
                toVertexBizId,
                (0 until propertySize).map(_ => Property.apply(binary)).toList,
                vidGenerator,
                bitMod.crudMode)
  }
}
