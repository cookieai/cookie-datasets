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

package ai.cookie.spark.sql.sources.iris

import ai.cookie.spark.sql.sources.SharedSQLContext
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NominalAttribute}
import org.apache.spark.sql.types.StructField
import org.scalatest.{FunSuite, Matchers}

class IrisRelationSuite extends FunSuite
with SharedSQLContext with Matchers {

  private val testDatasets = Seq(
    ("csv", new Path("src/test/resources/iris.data"), 150),
    ("libsvm", new Path("src/test/resources/iris.libsvm"), 300)
  )

  test("metadata") {
    for((format, path, _) <- testDatasets) {
      val df = sqlContext.read.iris(path.toString, format = format)

      def labels(field: StructField): Array[String] = {
        Attribute.fromStructField(field) match {
          case na: NominalAttribute if na.values.isDefined => na.values.get
        }
      }

      def features(field: StructField): Array[String] = {
        AttributeGroup.fromStructField(field).attributes match {
          case Some(a) => a.map(_.name.get)
          case None => Array()
        }
      }

      labels(df.schema("label")) should equal (IrisRelation.labels)
      features(df.schema("features")) should equal (IrisRelation.features)
    }
  }

  test("select") {
    for((format, path, numExamples) <- testDatasets) {
      val df = sqlContext.read.iris(path.toString, format = format)

      df.count() shouldEqual numExamples
      df.select("label").count() shouldEqual numExamples
      df.select("features").count() shouldEqual numExamples
      df.sample(false, 0.05).show()
    }
  }

  test("sql datasource") {
    for((format, path, numExamples) <- testDatasets) {
      sqlContext.sql(
        s"""
           |CREATE TEMPORARY TABLE iris
           |USING ai.cookie.spark.sql.sources.iris
           |OPTIONS (path "$path", format "${format}")
        """.stripMargin)

      val df = sqlContext.sql("SELECT * FROM iris")
      df.count() shouldEqual numExamples
      df.sample(false, 0.05).show()
    }
  }

  test("repeatability") {
    for((format, path, numExamples) <- testDatasets) {
      val df = sqlContext.read.iris(path.toString, format = format)

      df.count() shouldEqual numExamples
      df.count() shouldEqual numExamples
    }
  }

}
