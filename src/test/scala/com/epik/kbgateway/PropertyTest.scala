package com.epik.kbgateway

import com.epik.kbgateway.log.{Property, PropertyType}
import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.util.Random

class PropertyTest extends FlatSpec with BeforeAndAfter {

  "All kind of Properties Type" should "be serialize/deserialized" in {
    val properties =
      List(
        Property("some_string_property",
                 PropertyType.STRING,
                 Some(Random.alphanumeric.take(1000).mkString)),
        Property("some_int_property", PropertyType.INTEGER, Some(Random.nextInt(10000))),
        Property("some_double_property", PropertyType.DOUBLE, Some(Random.nextDouble()))
      )

    properties.foreach(p => {
      val binary               = p.getBytes
      val propertyDeserialized = Property.apply(binary)
      assert(propertyDeserialized.name.nonEmpty)

      assert(
        p == propertyDeserialized
      )
    })
  }

  "A Property" should "be copied" in {
    val property =
      Property("some_string_property",
               PropertyType.STRING,
               Some(Random.alphanumeric.take(1000).mkString))

    val copy1 = property.copy(defaultValueOpt = Some(Random.alphanumeric.take(1020).mkString))
    copy1.getBytes.array().size shouldBe copy1.totalLength
    val propertyDeserialized1 = Property.apply(copy1.getBytes)
    propertyDeserialized1 shouldBe copy1

    val copy2 = property.copy(defaultValueOpt = Some(Random.alphanumeric.take(1080).mkString))
    copy2.getBytes.array().size shouldBe copy2.totalLength
    val propertyDeserialized2 = Property.apply(copy2.getBytes)
    propertyDeserialized2 shouldBe copy2

  }
}
