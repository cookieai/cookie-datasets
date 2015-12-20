/*
 * Copyright 2015 cookie.ai
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package ai.cookie.spark.sql.sources.mnist

import java.io.EOFException

import ai.cookie.spark.ml.attribute.AttributeKeys
import ai.cookie.spark.sql.sources.SharedSQLContext
import ai.cookie.spark.sql.types.Conversions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.linalg.Vector
import org.scalatest.{FunSuite, Matchers}

class MnistRelationSuite extends FunSuite
with SharedSQLContext with Matchers {

  val labels = new Path("src/test/resources/t10k-labels-idx1-ubyte")
  val images = new Path("src/test/resources/t10k-images-idx3-ubyte")

  private class MnistSession(implicit conf: Configuration) {
    val imageParser = new MnistImageReader(images)
    val labelParser = new MnistLabelReader(labels)

    def close(): Unit = {
      Seq(imageParser, labelParser).foreach { _.close() }
    }
  }

  def imageStream(implicit session: MnistSession): Stream[Vector] = {
    def next(): Stream[Vector] = {
      try { (session.imageParser.next(): Vector) #:: next() }
      catch { case eof: EOFException => Stream.Empty }
    }
    next()
  }

  def labelStream(implicit session: MnistSession): Stream[Double] = {
    def next(): Stream[Double] = {
      try { session.labelParser.next().toDouble #:: next() }
      catch { case eof: EOFException => Stream.Empty }
    }
    next()
  }

  test("metadata") {
    val df = sqlContext.read.mnist(images.toString, labels.toString)
    val featureMetadata = df.schema("features").metadata

    featureMetadata.contains(AttributeKeys.SHAPE) shouldEqual true
    featureMetadata.getLongArray(AttributeKeys.SHAPE) should equal (Array(28L, 28L))
  }

  test("select") {
    val df = sqlContext.read.mnist(images.toString, labels.toString)

    df.count() shouldEqual 10000
    df.select("label").count() shouldEqual 10000
    df.select("features").count() shouldEqual 10000
    df.show(numRows = 3)
  }

  test("sql datasource") {
    sqlContext.sql(
      s"""
         |CREATE TEMPORARY TABLE t10k
         |USING ai.cookie.spark.sql.sources.mnist
         |OPTIONS (imagesPath "$images", labelsPath "$labels")
      """.stripMargin)

    val df = sqlContext.sql("SELECT * FROM t10k")
    df.count() shouldEqual 10000
    df.show(numRows = 3)
  }

  test("content") {
    val sc = sqlContext.sparkContext
    implicit val conf = sc.hadoopConfiguration

    val df = sqlContext.read.mnist(images.toString, labels.toString, Some(16 + (1000 * 768 - 1)))
      .select("label", "features")

    implicit val session = new MnistSession()
    try {
      val count = df.count().toInt
      count shouldEqual session.imageParser.numImages
      count shouldEqual session.labelParser.numLabels

      (labelStream zip imageStream zip df.rdd.toLocalIterator.toIterable) foreach {
        case ((label, features), row) => {
          row.getDouble(0) shouldEqual label
          row.get(1).asInstanceOf[Vector] shouldEqual features
        }
      }
    }
    finally {
      session.close()
    }

    df.stat.freqItems(Seq("label")).show
  }

  test("repeatability") {
    val df = sqlContext.read.mnist(images.toString, labels.toString)

    df.count() shouldEqual 10000
    df.count() shouldEqual 10000
  }

}
