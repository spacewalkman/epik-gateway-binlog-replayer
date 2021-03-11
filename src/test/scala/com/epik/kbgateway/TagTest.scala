package com.epik.kbgateway

import com.epik.kbgateway.log.NebulaGraphSchema.DefaultVertexTagName
import com.epik.kbgateway.log.{NebulaGraphSchema, Property, PropertyType, Tag}
import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.util.Random

class TagTest extends FlatSpec with BeforeAndAfter {

  "All kind of Tags Type" should "be serialize/deserialized" in {
    val properties =
      List(
        Property("some_string_property",
                 PropertyType.STRING,
                 Some(Random.alphanumeric.take(1000).mkString)),
        Property("some_int_property", PropertyType.INTEGER, Some(Random.nextInt(10000))),
        Property("some_double_property", PropertyType.DOUBLE, Some(Random.nextDouble()))
      )

    val tag             = Tag("some_tag", properties)
    val binary          = tag.getBytes
    val tagDeserialized = Tag.apply(binary)

    assert(
      tag == tagDeserialized
    )
  }

  "A Tag with chinese character" should "be serialize/deserialized" in {
    val copy1 = NebulaGraphSchema.DefaultTag.copy(
      properties = List(NebulaGraphSchema.DefaultProperty.copy(
        defaultValueOpt = Some(Random.alphanumeric.take(1000).mkString))))

    copy1.getBytes.array().size shouldBe copy1.totalLength
    val tagDeserialized1 = Tag.apply(copy1.getBytes)
    tagDeserialized1 shouldBe copy1

    val tagObject =
      Tag(
        "abcdefghijklmn中文字__!@#$%^&*()_+abcdefghijklmn_中文字",
        List(
          Property("abcdefghijklmn中文字__!@#$%^&*()_+abcdefghijklmn_中文字",
                   PropertyType.STRING,
                   Some("abcdefghijklmn中文字__!@#$%^&*()_+abcdefghijklmn_中文字")))
      )
    tagObject.getBytes.array().size shouldBe tagObject.totalLength
    val tagDeserialized2 = Tag.apply(tagObject.getBytes)
    tagDeserialized2 shouldBe tagObject

  }
}
