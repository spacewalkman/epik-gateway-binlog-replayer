package com.epik.kbgateway.log

import com.vesoft.nebula.client.graph.NebulaPoolConfig
import com.vesoft.nebula.client.graph.data.HostAddress
import com.vesoft.nebula.client.graph.net.NebulaPool
import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.util.Random

class NebulaGraphSchemaTest extends FlatSpec with BeforeAndAfter {

  "All kind of Log" should "be formatted to Nebula nGQL" in {
    val properties =
      List(
        Property("some_string_property",
                 PropertyType.STRING,
                 Some(Random.alphanumeric.take(1000).mkString)),
        Property("some_int_property", PropertyType.INTEGER, Some(Random.nextInt(10000))),
        Property("some_double_property", PropertyType.DOUBLE, Some(Random.nextDouble()))
      )
    val tag = Tag("some_tag_name", properties)

    val logs = List(
      SchemaGraphDbLog("some_graph_db_name", 1024, 3),
      SchemaGraphDbLog("some_graph_db_name", 1024, 3, VidGeneratorType.HASH, CrudMode.DELETE),
      SchemaTagLog("some_graph_db_name", "vertex_tag", properties),
      SchemaTagLog("some_graph_db_name", "vertex_tag", properties, CrudMode.DELETE),
      SchemaEdgeLog("some_graph_db_name", "some_edge_name", properties),
      SchemaEdgeLog("some_graph_db_name", "some_edge_name", properties, CrudMode.DELETE),
      DataVertexLog("some_graph_db_name", "vertex_business_id", List(tag)),
      DataVertexLog("some_graph_db_name", "vertex_business_id", List(tag), VidGeneratorType.HASH),
      DataVertexLog("some_graph_db_name",
                    "vertex_business_id",
                    List(tag),
                    VidGeneratorType.FIX_LENGTH_STRING,
                    CrudMode.DELETE),
      DataEdgeLog("some_graph_db_name",
                  "some_edge_name",
                  "from_vertex_business_id",
                  "to_vertex_business_id",
                  properties,
                  VidGeneratorType.HASH),
      DataEdgeLog("some_graph_db_name",
                  "some_edge_name",
                  "from_vertex_business_id",
                  "to_vertex_business_id",
                  properties,
                  VidGeneratorType.HASH,
                  CrudMode.DELETE),
      DataEdgeLog("some_graph_db_name",
                  "some_edge_name",
                  "from_vertex_business_id",
                  "to_vertex_business_id",
                  properties,
                  VidGeneratorType.FIX_LENGTH_STRING),
      DataEdgeLog("some_graph_db_name",
                  "some_edge_name",
                  "from_vertex_business_id",
                  "to_vertex_business_id",
                  properties,
                  VidGeneratorType.FIX_LENGTH_STRING,
                  CrudMode.DELETE)
    )

    logs.map(log => NebulaGraphSchema.formatSql(log)).foreach(println)

  }

  "Default schema" should "be generated" in {
    val ngql = NebulaGraphSchema.defaultCreateSchemaSql

    val expectedSql =
      List(
        "CREATE SPACE IF NOT EXISTS cn_dbpedia(partition_num=128, replica_factor=3, vid_type=INT64)",
        "CREATE TAG IF NOT EXISTS domain_entity(domain_predicate string)",
        "CREATE EDGE IF NOT EXISTS predicate(domain_predicate string)"
      )

    ngql shouldBe expectedSql.mkString(";")
  }

  "Custom schema" should "be generated" in {
    val ngql = NebulaGraphSchema.customSchemaSql("cn_dbpedia", 512, 5)

    val expectedSql =
      List(
        "CREATE SPACE IF NOT EXISTS cn_dbpedia(partition_num=512, replica_factor=5, vid_type=INT64)",
        "CREATE TAG IF NOT EXISTS domain_entity(domain_predicate string)",
        "CREATE EDGE IF NOT EXISTS predicate(domain_predicate string)"
      )

    ngql shouldBe expectedSql.mkString(";")
  }

  "Nebula java client" should "be used" in {
    val nebulaPoolConfig = new NebulaPoolConfig
    nebulaPoolConfig.setMaxConnSize(10)
    nebulaPoolConfig.setTimeout(1000)
    val addresses =
      java.util.Arrays
        .asList(new HostAddress("127.0.0.1", 9669), new HostAddress("127.0.0.1", 9669))
    val pool = new NebulaPool
    pool.init(addresses, nebulaPoolConfig)

    val session = pool.getSession("root", "nebula", false)
    val rs      = session.execute("SHOW HOSTS;")
    assert(!rs.isEmpty)
    session.release

  }
}
