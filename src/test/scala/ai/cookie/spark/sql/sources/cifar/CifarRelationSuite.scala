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

package ai.cookie.spark.sql.sources.cifar

import java.io.EOFException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.attribute.{NominalAttribute, Attribute}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructField
import ai.cookie.spark.ml.attribute.AttributeKeys
import ai.cookie.spark.ml.feature.IndexToString
import ai.cookie.spark.sql.sources.DataSourceTest
import org.scalatest.{BeforeAndAfter, Matchers}
import ai.cookie.spark.sql.types.Conversions._

class CifarRelationSuite extends DataSourceTest with SharedSQLContext
  with BeforeAndAfter with Matchers {

  private val testDatasets = Seq(
    (CifarFormats._100, new Path("src/test/resources/cifar-100-binary/sample.bin"), 100),
    (CifarFormats._10, new Path("src/test/resources/cifar-10-batches-bin/sample.bin"), 100)
  )

  private def recordStream(implicit parser: CifarReader): Stream[CifarRecord] = {
    def next(): Stream[CifarRecord] = {
      try { (parser.next(): CifarRecord) #:: next() }
      catch { case eof: EOFException => Stream.Empty }
    }
    next()
  }

  private def show(df: DataFrame) = {
    val t = new IndexToString()
    t.setInputCol("label").setOutputCol("labelName")
    t.transform(df).show(numRows = 10)
  }
  override def beforeAll(): Unit = {
      super.beforeAll()
  }

  test("metadata") {
    for((format, path, _) <- testDatasets) {
      val df = sqlContext.read.cifar(path.toString, format.name)

      println(df.schema.json)

      def values(field: StructField) = {
        Attribute.fromStructField(field) match {
          case na: NominalAttribute => na.values
          case _ => None
        }
      }

      format match {
        case CifarFormats._10 =>
          values(df.schema("label")) should equal(Some(CifarFormats._10.labels))
        case CifarFormats._100 =>
          values(df.schema("label")) should equal(Some(CifarFormats._100.fineLabels))
          values(df.schema("coarseLabel")) should equal(Some(CifarFormats._100.coarseLabels))
      }

      val featureMetadata = df.schema("features").metadata
      featureMetadata.contains(AttributeKeys.SHAPE) shouldEqual true
      featureMetadata.getLongArray(AttributeKeys.SHAPE) should equal(Array(3L, 32L, 32L))
    }
  }

  test("select") {
    for((format, path, count) <- testDatasets) {
      val df = sqlContext.read.cifar(path.toString, format.name)

      //df.rdd.partitions.length shouldEqual 10
      df.count() shouldEqual count
      df.select("label").count() shouldEqual count
      df.select("features").count() shouldEqual count
      show(df)
    }
  }

  test("sql datasource") {
    for((format, path, count) <- testDatasets) {
      sqlContext.sql(
        s"""
           |CREATE TEMPORARY TABLE cifar
           |USING ai.cookie.spark.sql.sources.cifar
           |OPTIONS (path "$path", format "${format.name}")
      """.stripMargin)

      val df = sqlContext.sql("SELECT * FROM cifar")
      df.count() shouldEqual count
      show(df)

    }
  }

  test("content") {
    val sc = sqlContext.sparkContext
    implicit val conf = sc.hadoopConfiguration

    for((format, path, count) <- testDatasets) {
      format match {
        case CifarFormats._10 =>
          val df = sqlContext.read.cifar(path.toString, format.name, Some(Long.MaxValue /*30741338L*/))
            .select("label", "features")

          implicit val parser = new CifarReader(path, format, true)
          try {
            val count = df.count().toInt
            count shouldEqual count

            (recordStream zip df.rdd.toLocalIterator.toIterable) foreach {
              case (record, row) => {
                row.getDouble(0) shouldEqual record.fineLabel
                row.get(1).asInstanceOf[Vector] shouldEqual (record.image: Vector)
              }
            }
          }
          finally {
            parser.close()
          }

          df.stat.freqItems(Seq("label")).show

        case CifarFormats._100 =>
          val df = sqlContext.read.cifar(path.toString, format.name, Some(Long.MaxValue /*30741338L*/))
            .select("coarseLabel", "label", "features")

          implicit val parser = new CifarReader(path, format, true)
          try {
            val count = df.count().toInt
            count shouldEqual count

            (recordStream zip df.rdd.toLocalIterator.toIterable) foreach {
              case (record, row) => {
                row.getDouble(0) shouldEqual record.coarseLabel
                row.getDouble(1) shouldEqual record.fineLabel
                row.get(2).asInstanceOf[Vector] shouldEqual (record.image: Vector)
              }
            }
          }
          finally {
            parser.close()
          }

          df.stat.freqItems(Seq("label")).show
      }
    }
  }

  test("repeatability") {
    for((format, path, count) <- testDatasets) {
      val df = sqlContext.read.cifar(path.toString, format.name)
      (1 to 2) foreach { _ =>
        df.count() shouldEqual count
      }
    }
  }

}
