package com.epik.kbgateway

import java.nio.{BufferOverflowException, BufferUnderflowException, ByteBuffer}

import com.epik.kbgateway.log.{Log, NebulaGraphSchema}
import com.vesoft.nebula.client.graph.NebulaPoolConfig
import com.vesoft.nebula.client.graph.data.HostAddress
import com.vesoft.nebula.client.graph.net.NebulaPool
import org.apache.commons.cli.{
  CommandLine,
  DefaultParser,
  HelpFormatter,
  Options,
  ParseException,
  Option => CliOption
}
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * 1） Interpret binary to XXXLog, use XXXLog to format sql.
  * 2） Call Nebula Java client, to batch execute sqls.
  */
object LogFileReplayer {

  val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    lazy val options: Options = {
      val inputPath = CliOption
        .builder("i")
        .longOpt("input")
        .required()
        .hasArg()
        .desc("Input file path.")
        .build

      val database = CliOption
        .builder("d")
        .longOpt("database")
        .required
        .hasArg()
        .desc("Nebula Graph database name.")
        .build

      val graphServers = CliOption
        .builder("h")
        .longOpt("graphd")
        .required
        .hasArg()
        .desc(
          "Comma separated nebula graphd servers socket addresses."
        )
        .build

      val bathSize = CliOption
        .builder("b")
        .longOpt("batchSize")
        .`type`(classOf[Int])
        .hasArg()
        .desc(
          "How many logs be batched into a ngql."
        )
        .build

      val maxConnectionPerGroup = CliOption
        .builder("s")
        .longOpt("poolSize")
        .`type`(classOf[Int])
        .hasArg()
        .desc(
          "Nebula max connections per group."
        )
        .build

      val nebulaConnectionTimeout = CliOption
        .builder("t")
        .longOpt("timeout")
        .`type`(classOf[Int])
        .hasArg()
        .desc(
          "Nebula Connection timeout in milliseconds."
        )
        .build

      val user = CliOption
        .builder("u")
        .longOpt("user")
        .hasArg()
        .desc(
          "User to connect Nebula Graph."
        )
        .build

      val password = CliOption
        .builder("w")
        .longOpt("password")
        .hasArg()
        .desc(
          "Password to connect Nebula Graph."
        )
        .build

      val ngsqlOutput = CliOption
        .builder("q")
        .longOpt("ngqlOutput")
        .hasArg()
        .desc(
          "Ngsql output path."
        )
        .build

      val opts = new Options()
      opts.addOption(inputPath)
      opts.addOption(graphServers)
      opts.addOption(database)
      opts.addOption(bathSize)
      opts.addOption(user)
      opts.addOption(password)
      opts.addOption(maxConnectionPerGroup)
      opts.addOption(nebulaConnectionTimeout)
      opts.addOption(ngsqlOutput)
    }

    lazy val formatter = {
      val format = new HelpFormatter
      format.setWidth(300)
      format
    }

    val parser = new DefaultParser

    var cmd: CommandLine = null
    try {
      cmd = parser.parse(options, args)
    } catch {
      case e: ParseException => {
        log.error("Illegal arguments", e)
        formatter.printHelp("LogFileReplayer", options)
        System.exit(-1)
      }
    }

    var database: String = cmd.getOptionValue("d")
    if (database == null || database.isEmpty) {
      database = NebulaGraphSchema.DefaultGraphDbName
    }

    val inputPath: String = cmd.getOptionValue("i")

    val graphdStrs: String = cmd.getOptionValue("h")

    var user: String = cmd.getOptionValue("u")
    if (user == null || user.isEmpty) {
      user = "user"
    }

    var password: String = cmd.getOptionValue("w")
    if (password == null || password.isEmpty) {
      password = "password"
    }

    val portStr: String = cmd.getOptionValue("p")
    var port            = 9669 //default nebula graphd port when using docker-compose
    if (portStr != null && !portStr.isEmpty) {
      try {
        port = portStr.toInt
      } catch {
        case e: NumberFormatException => {
          log.error("Illegal port format, fall back to 3699", e)
        }
      }
    }

    val batchSizeStr: String = cmd.getOptionValue("b")
    var bathSize             = 1000
    if (batchSizeStr != null && !batchSizeStr.isEmpty) {
      try {
        bathSize = batchSizeStr.toInt
      } catch {
        case e: NumberFormatException => {
          log.error("Illegal bathSize format, fall back to 1000", e)
        }
      }
    }

    val maxConnectionPerGroupStr: String = cmd.getOptionValue("s")
    var maxConnectionPerGroup            = 1
    if (maxConnectionPerGroupStr == null || maxConnectionPerGroupStr.isEmpty) {
      try {
        maxConnectionPerGroup = maxConnectionPerGroupStr.toInt
      } catch {
        case e: NumberFormatException => {
          log.error("Illegal maxConnectionPerGroup format, fall back to 1", e)
        }
      }
    }

    val nebulaConnectionTimeoutStr: String = cmd.getOptionValue("t")
    var nebulaConnectionTimeout            = 3000
    if (nebulaConnectionTimeoutStr == null || nebulaConnectionTimeoutStr.isEmpty) {
      try {
        nebulaConnectionTimeout = nebulaConnectionTimeoutStr.toInt
      } catch {
        case e: NumberFormatException => {
          log.error("Illegal nebulaConnectionTimeout format, fall back to 3000", e)
        }
      }
    }

    var ngqlOutput: String = cmd.getOptionValue("g")
    if (ngqlOutput == null || ngqlOutput.isEmpty) {
      ngqlOutput = "ngql_output"
    }

    val sparkConf = new SparkConf().setAppName("LogFileReplayer")
    sparkConf.set("mapreduce.output.fileoutputformat.compress", "false")
    // needed by Serialize XXXWritable
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)

    val sqlRDD: RDD[String] =
      sc.sequenceFile[NullWritable, BytesWritable](inputPath)
        .mapPartitions(iter => {
          val log2Sqls: List[String] =
            iter.map {
              case (_, b) => {
                val buffer = ByteBuffer.wrap(b.getBytes())
                // one log per line, ignore remaining padding.

                var sql: String = ""
                try {
                  val epikLog = Log.parse(buffer)
                  //log.info(s"Parsed a Log:${epikLog}")
                  sql = NebulaGraphSchema.formatSql(epikLog)
                } catch {
                  case e @ (_: BufferOverflowException | _: BufferUnderflowException) => {
                    log.error(s"Buffer exceptions", e)
                  }
                }
                //log.info(s"${sql}")
                sql
              }
            }.toList // avoid iterator is spend, which result in sqlRDD be empty.

          //Prepend "USE graph_database_XXX" before actual sql.
          val ngqls =
            log2Sqls
              .map(_.trim)
              .filter(_.nonEmpty)
              .grouped(bathSize)
              .map(x => x.mkString(s"USE ${database};", ";", ";"))
              .toList
          //log.info(s"sql count: ${ngqls.size}, group_size=${bathSize}")

          // HostAddress not Serializable, so must be put in closure
          val hostAddrs: Array[HostAddress] = graphdStrs
            .split(",")
            .map(a =>
              a.split(":").toList match {
                case host :: port :: Nil =>
                  new HostAddress(host, port.toInt)
                case _ =>
                  throw new IllegalArgumentException(s"Illegal graphd address:${graphdStrs}")
            })

          try {
            val eitherOrNebulaPool =
              initNebulaPool(hostAddrs, maxConnectionPerGroup, nebulaConnectionTimeout)

            eitherOrNebulaPool match {
              case Right(pool) => {
                //one session per partition
                val session = pool.getSession(user, password, false)
                ngqls.foreach { ngql =>
                  {
                    //log.info(s"Ready to execute ngql:${ngql}, statement count:${ngql.split(";").size}")
                    val rs = session.execute(ngql)
                    if (!rs.isSucceeded()) {
                      log.error(s"Ngql execution failed: ${rs.getErrorMessage()}")
                    } else {
                      log.info("Ngql execution success.")
                    }
                  }
                }

                session.release()
              }
              case Left(_) =>
                /*do nothing*/
                log.trace("Entering Left branch, Nebula Pool connection failed.")
            }
          } catch {
            case e: Exception => {
              log.error("Execute ngql error.", e)
            }
          }

          ngqls.iterator
        })

    //save ngql scripts.
    sqlRDD.saveAsTextFile(ngqlOutput)
    log.info(s"Save ngql to ${ngqlOutput} successfully.")

    sc.stop()
  }

  def initNebulaPool(graphdSockets: Array[HostAddress],
                     maxConnection: Int,
                     nebulaConnectionTimeout: Int): Either[Boolean, NebulaPool] = {
    require(graphdSockets != null && graphdSockets.size > 0)
    val nebulaPoolConfig = new NebulaPoolConfig
    nebulaPoolConfig.setMaxConnSize(maxConnection)
    nebulaPoolConfig.setTimeout(nebulaConnectionTimeout)
    val pool   = new NebulaPool
    val initOk = pool.init(java.util.Arrays.asList(graphdSockets: _*), nebulaPoolConfig)
    if (!initOk) {
      log.warn(s"Cannot init nebula pool on ${graphdSockets.map(_.toString).mkString(",")}")
      pool.close()
      Left(initOk)
    } else {
      Right(pool)
    }
  }
}
