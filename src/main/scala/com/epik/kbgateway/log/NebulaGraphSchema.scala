package com.epik.kbgateway.log

object NebulaGraphSchema {

  lazy val CreateGraphDbLog =
    SchemaGraphDbLog(DefaultGraphDbName, DefaultPartition, DefaultReplicas)

  /**
    * 1 tag type only, for Subject and Object of SPO triple.
    */
  lazy val CreateTagLog =
    SchemaTagLog(DefaultGraphDbName, DefaultVertexTagName, List(DefaultProperty))

  /**
    * for tuple3 has 1 property, tuple4 has 2 properties.
    */
  lazy val CreateEdgeLog =
    SchemaEdgeLog(DefaultGraphDbName, DefaultEdgeName, List(DefaultProperty))

  /**
    * Default schema ngql
    */
  lazy val defaultCreateSchemaSql: String =
    List(CreateGraphDbLog, CreateTagLog, CreateEdgeLog)
      .map(log => NebulaGraphSchema.formatSql(log))
      .mkString(";")

  /**
    * Default Graph database related.
    */
  val DefaultGraphDbName     = "cn_dbpedia"
  val DefaultPartition: Int  = 1024
  val DefaultReplicas: Short = 3
  val DefaultVertexTagName   = "domain_entity"
  val DefaultPropertyName    = "domain_predicate"

  /**
    * cn_dbpedia is triples, has 1 edge type only.
    */
  val DefaultEdgeName         = "predicate"
  val DefaultEdgePropertyName = "edge_property"

  /**
    * By default a vertex has only 1 Tag, which has only 1 Property. And an Edge has only 1 property either.
    */
  val DefaultProperty = Property(DefaultPropertyName, PropertyType.STRING, None)

  /**
    * when tuple4, the 3rd element as an edge property.
    */
  val DefaultEdgeProperty = Property(DefaultEdgePropertyName, PropertyType.STRING, None)

  /**
    * Domain entity include Subject & Object of a S-P-O triple
    */
  val DefaultTag = Tag(DefaultVertexTagName, List(DefaultProperty))

  //For concatenation of "IF NOT EXIST"
  val IfNotExistOption = Some("IF NOT EXISTS")
  val IfExistOption    = Some("IF EXISTS")

  /**
    * custom create schema
    */
  def customSchemaSql(graphDbName: String, partition: Int, replicas: Short): String =
    List(SchemaGraphDbLog(graphDbName, partition, replicas), CreateTagLog, CreateEdgeLog)
      .map(log => NebulaGraphSchema.formatSql(log))
      .mkString(";")

  //nebula ngql grammar: https://docs.nebula-graph.io/2.0/3.ngql-guide/2.nav.md/1.search-guide/
  def formatSql(log: Log): String = log match {
    case SchemaGraphDbLog(graphDbName, partitionNumber, replicas, vidGeneratorType, crudMode) =>
      assert(vidGeneratorType == VidGeneratorType.HASH)
      crudMode match {
        case CrudMode.ADD =>
          s"CREATE SPACE ${IfNotExistOption.getOrElse("")} ${graphDbName}(partition_num=${partitionNumber}, replica_factor=${replicas}, vid_type=INT64)"
        case CrudMode.DELETE =>
          s"DROP SPACE ${IfExistOption.getOrElse("")} ${graphDbName}"
        case _ => throw new IllegalStateException(s"Unsupported ${crudMode} of SchemaGraphDbLog.")
      }

    case SchemaTagLog(graphDbName, tagName, properties, crudMode) => {
      crudMode match {
        case CrudMode.ADD =>
          s"CREATE TAG ${IfNotExistOption.getOrElse("")} ${tagName}${properties
            .map(p => s"${p.name} ${p.`type`}${p.wrappedDefaultValue.fold("")(v => " DEFAULT " + v)}")
            .mkString("(", ",", ")")}"
        case CrudMode.DELETE =>
          s"DROP TAG ${IfExistOption.getOrElse("")} ${tagName}"
        case _ => throw new IllegalStateException(s"Unsupported ${crudMode} of SchemaTagLog.")
      }
    }
    case SchemaEdgeLog(graphDbName, edgeName, properties, crudMode) => {
      crudMode match {
        case CrudMode.ADD =>
          s"CREATE EDGE ${IfNotExistOption.getOrElse("")} ${edgeName}(${properties
            .map(p => s"${p.name} ${p.`type`}${p.wrappedDefaultValue.fold("")(v => " DEFAULT " + v)}")
            .mkString(",")})"
        case CrudMode.DELETE =>
          s"DROP EDGE ${IfExistOption.getOrElse("")} ${edgeName}"
        case _ => throw new IllegalStateException(s"Unsupported ${crudMode} of SchemaEdgeLog.")
      }
    }
    case DataVertexLog(graphDbName, vertexBizId, tags, vidGenerator, crudMode) => {
      val vid = vidGenerator match {
        case VidGeneratorType.HASH              => raw"""hash("${vertexBizId}")"""
        case VidGeneratorType.FIX_LENGTH_STRING => raw""""${vertexBizId}""""
      }

      crudMode match {
        case CrudMode.ADD =>
          s"INSERT VERTEX ${tags
            .map(t => s"${t.name} ${t.properties.map(_.name).mkString("(", ",", ")")}")
            .mkString(",")}" + s" VALUES ${vid}:${tags
            .map(t => t.properties.map(_.wrappedDefaultValue.get).mkString("(", ",", ")"))
            .mkString(",")}"
        case CrudMode.DELETE =>
          s"DROP VERTEX ${vid}"
        case _ => throw new IllegalStateException(s"Unsupported ${crudMode} of DataVertexLog.")
      }
    }

    case DataEdgeLog(graphDbName,
                     edgeName,
                     fromVertexBizId,
                     toVertexBizId,
                     properties,
                     vidGenerator,
                     crudMode) => {
      val (fromVID, toVID) = vidGenerator match {
        case VidGeneratorType.HASH =>
          raw"""hash("${fromVertexBizId}")""" -> raw"""hash("${toVertexBizId}")"""
        case VidGeneratorType.FIX_LENGTH_STRING =>
          raw""""${fromVertexBizId}"""" -> raw""""${toVertexBizId}""""
      }

      crudMode match {
        case CrudMode.ADD =>
          s"INSERT EDGE ${edgeName} ${properties.map(_.name).mkString("(", ",", ")")} VALUES ${fromVID}->${toVID}:${properties
            .map(_.wrappedDefaultValue.get)
            .mkString("(", ",", ")")}"
        case CrudMode.DELETE =>
          s"DROP EDGE ${fromVID}->${toVID}"
        case _ => throw new IllegalStateException(s"Unsupported ${crudMode} of DataEdgeLog.")
      }
    }
  }

  def defaultTripleSchemaLogs(tupleSize: Int) = tupleSize match {
    case 3 => List(CreateGraphDbLog, CreateTagLog, CreateEdgeLog)
    case 4 =>
      List(CreateGraphDbLog,
           CreateTagLog,
           CreateEdgeLog.copy(properties = List(DefaultProperty, DefaultEdgeProperty)))
    case _ => throw new IllegalArgumentException(s"Unsupported Tuple Size:${tupleSize}")
  }
}
