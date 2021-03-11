package com.epik.kbgateway

import com.epik.kbgateway.log.{CrudMode, DbTagEdgeMode, SchemaDataMode, _}
import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.collection.SortedSet

class BitModeTest extends FlatSpec with BeforeAndAfter {

  "All kind of BitMode Type" should "be serialize/deserialized" in {
    val combination: SortedSet[((SchemaDataMode.Value, DbTagEdgeMode.Value), CrudMode.Value)] =
      SchemaDataMode.values.zip(DbTagEdgeMode.values).zip(CrudMode.values)

    combination.map {
      case ((schemaDataMode, dbTagEdgeMode), crudMode) =>
        val bitMode = BitMode(schemaDataMode, dbTagEdgeMode, crudMode)

        val bt                  = bitMode.getByte
        val bitModeDeserialized = BitMode.apply(bt)

        assert(
          bitMode == bitModeDeserialized
        )
    }

  }
}
