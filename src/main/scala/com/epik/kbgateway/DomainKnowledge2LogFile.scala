package com.epik.kbgateway

import com.epik.kbgateway.log.NebulaGraphSchema.{DefaultPropertyName, DefaultVertexTagName}
import com.epik.kbgateway.log._
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
  * Domain Knowledge(Triplet) file to log file converter
  */
object DomainKnowledge2LogFile {

  /**
    * default cn_dbpedia triplet file name
    */
  val DefaultCnDbPediaFileName = "baike_triples.txt"
  val DefaultSeparator         = "\t"

  /**
    * Default tuple size(column size) on a row,default to Triple=3
    */
  val DefaultTupleSize = 3

  private[this] val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    lazy val options: Options = {
      // date partition field, format=yyyy-MM-dd
      val triplesFile = CliOption
        .builder("f")
        .longOpt("domain_triplet_file")
        .hasArg()
        .desc("Domain triplet file location.")
        .build

      val separator = CliOption
        .builder("s")
        .longOpt("separator")
        .hasArg()
        .desc("Separator to split triplet files.")
        .build

      val database = CliOption
        .builder("d")
        .longOpt("database")
        .required
        .hasArg()
        .desc("Graph database name.")
        .build

      val vidGenerator = CliOption
        .builder("v")
        .longOpt("vertex_id_generator")
        .hasArg()
        .desc("Vertex id generator scheme, currently support HASH and STRING scheme,may be specified as HASH/STRING case-insensitively.")
        .build

      val tupleSize = CliOption
        .builder("z")
        .longOpt("tuple_size")
        .hasArg()
        .desc("Tuple size on a row,default to Triple=3")
        .build

      val limit = CliOption
        .builder("l")
        .longOpt("limit")
        .hasArg()
        .desc("Taken first N only, used in test.")
        .build

      val dryRun = CliOption
        .builder("r")
        .longOpt("dry_run")
        .`type`(classOf[Boolean])
        .desc("Dry run, which only parse triplet file to Log instance, not generate Log files.")
        .build

      val output = CliOption
        .builder("t")
        .longOpt("output")
        .hasArg()
        .desc("Where is the Log files goes to, only take effect when dry_run=false.")
        .build

      val opts = new Options()
      opts.addOption(triplesFile)
      opts.addOption(separator)
      opts.addOption(database)
      opts.addOption(vidGenerator)
      opts.addOption(tupleSize)
      opts.addOption(limit)
      opts.addOption(dryRun)
      opts.addOption(output)
    }

    // cmd line formatter when something is wrong with options
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
        formatter.printHelp("DomainKnowledge2LogFile", options)
        System.exit(-1)
      }
    }

    var tripleFile: String = cmd.getOptionValue("f")
    if (tripleFile == null || tripleFile.isEmpty) {
      tripleFile = DefaultCnDbPediaFileName
    }

    var database: String = cmd.getOptionValue("d")
    if (database == null || database.isEmpty) {
      database = NebulaGraphSchema.DefaultGraphDbName
    }

    val tupleSizeStr: String = cmd.getOptionValue("z")
    val tupleSize = if (tupleSizeStr == null || tupleSizeStr.isEmpty) {
      DefaultTupleSize
    } else {
      tupleSizeStr.toInt
    }

    val vidGeneratorStr: String = cmd.getOptionValue("v")
    val vidGenerator: VidGeneratorType.Value = if (vidGeneratorStr == null) {
      VidGeneratorType.HASH
    } else {
      if (vidGeneratorStr.compareToIgnoreCase("hash") == 0) {
        VidGeneratorType.HASH
      } else if (vidGeneratorStr.compareToIgnoreCase("string") == 0) {
        VidGeneratorType.FIX_LENGTH_STRING
      } else {
        throw new IllegalArgumentException(
          s"Unsupported VID generator scheme: ${vidGeneratorStr}, only HASH and STRING is supported.")
      }
    }

    var separator: String = cmd.getOptionValue("s")
    if (separator == null) {
      separator = DefaultSeparator
    }

    val isDryRun: Boolean = cmd.hasOption('r')

    val limitStr: String = cmd.getOptionValue("l")
    val limit: Option[Int] = if (limitStr == null || limitStr.isEmpty) {
      None
    } else {
      Some(limitStr.toInt)
    }

    var logFileOutput: String = cmd.getOptionValue("t")
    if (logFileOutput == null || logFileOutput.isEmpty) {
      logFileOutput = "epik_log_file"
    }

    val sparkConf = new SparkConf().setAppName("DomainKnowledge2LogFile")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("mapreduce.output.fileoutputformat.compress", "false")
    val sc = new SparkContext(sparkConf)

    val df: RDD[Log] =
      sc.textFile(tripleFile)
        .map(_.split(separator).map(_.trim()).filter(_.nonEmpty).toList) // trim each tuple element
        .filter(_.size == tupleSize)
        .map {
          case subject :: predicate :: obj :: Nil =>
            val escapedSubject   = escape(subject)
            val escapedPredicate = escape(predicate)
            val escapedObj       = escape(obj)

            // may be empty after escape
            if (escapedSubject.isEmpty || escapedPredicate.isEmpty || escapedObj.isEmpty) {
              List.empty[Log]
            } else {
              val tagSubject =
                Tag(DefaultVertexTagName,
                    List(Property(DefaultPropertyName, PropertyType.STRING, Some(escapedSubject))))

              val tagObject =
                Tag(DefaultVertexTagName,
                    List(Property(DefaultPropertyName, PropertyType.STRING, Some(escapedObj))))

              val predicateProperty =
                Property(DefaultPropertyName, PropertyType.STRING, Some(escapedPredicate))

              //one triple decompose into 2 vertexes and 1 edge connecting them.
              List[Log](
                DataVertexLog(
                  database,
                  escapedSubject,
                  List(tagSubject),
                  vidGenerator
                ),
                DataVertexLog(
                  database,
                  escapedObj,
                  List(tagObject),
                  vidGenerator
                ),
                DataEdgeLog(database,
                            NebulaGraphSchema.DefaultEdgeName,
                            escapedSubject,
                            escapedObj,
                            List(predicateProperty),
                            vidGenerator)
              )
            }
          // when tuple4, 3rd element assume as edge's property
          case subject :: predicate :: edgeProperty :: obj :: Nil =>
            val escapedSubject      = escape(subject)
            val escapedEdgeProperty = escape(edgeProperty)
            val escapedPredicate    = escape(predicate)
            val escapedObj          = escape(obj)

            // may be empty after escape
            if (escapedSubject.isEmpty || escapedEdgeProperty.isEmpty || escapedPredicate.isEmpty || escapedObj.isEmpty) {
              List.empty[Log]
            } else {
              List[Log](
                DataVertexLog(
                  database,
                  escapedSubject,
                  List(
                    NebulaGraphSchema.DefaultTag.copy(
                      properties = List(NebulaGraphSchema.DefaultProperty.copy(defaultValueOpt =
                        Some(escapedSubject))))),
                  vidGenerator
                ),
                DataVertexLog(
                  database,
                  escapedObj,
                  List(NebulaGraphSchema.DefaultTag.copy(properties = List(
                    NebulaGraphSchema.DefaultProperty.copy(defaultValueOpt = Some(escapedObj))))),
                  vidGenerator
                ),
                // when tuple4, an edge has 2 properties.
                DataEdgeLog(
                  database,
                  NebulaGraphSchema.DefaultEdgeName,
                  escapedSubject,
                  escapedObj,
                  List(
                    NebulaGraphSchema.DefaultProperty.copy(
                      defaultValueOpt = Some(escapedPredicate)),
                    NebulaGraphSchema.DefaultEdgeProperty.copy(defaultValueOpt =
                      Some(escapedEdgeProperty))
                  ),
                  vidGenerator
                )
              )
            }
          case _ =>
            throw new IllegalStateException(
              "Illegal tuple format, only tuple 3 or 4 supported now.")
        }
        .filter(_.nonEmpty)
        .flatMap(x => x)
        .distinct()
        .sortBy(x => x, false) // Descending order, make Vertex creation comes before Edge's, make sure Edge's bounding Vertexes exists when inserting.

    //    log.info(s"Loading ${tripleFile}, count:${df
    //      .count()},separator=${separator},tupleSize=${tupleSize},isDryRun=${isDryRun}")

    val byteLogsRDD: RDD[Array[Byte]] = df
      .map(log => {
        val buffer        = log.getBytes
        val logParsedBack = Log.parse(buffer)
        assert(logParsedBack == log, s"Expected log:${log}, Actual Log:${logParsedBack}")

        buffer.array()
      })

    if (!isDryRun) {
      byteLogsRDD
        .map(a => (NullWritable.get(), new BytesWritable(a)))
        .saveAsSequenceFile(logFileOutput)
    } else {
      log.debug("Dry Running...")
      //when dryRun, and limit is defined, print out Log toString.
      if (limit.isDefined) {
        df.take(limit.get).foreach(println)
      }
    }

    sc.stop()
  }

  /**
    * escape "()\"\'", which will cause grammar error when generating ngql.
    * TODO: Is there is anything else need to escape?
    */
  def escape(input: String): String =
    input
      .replace("(", "\\(")
      .replace(")", "\\)")
      .replace("\"", "\\\"")
      .replace("\'", "\\\'")
      .replace("`", "\\`")
      .replace("�", "")
}
