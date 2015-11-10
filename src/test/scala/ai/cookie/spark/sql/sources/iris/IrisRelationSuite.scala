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

import org.apache.hadoop.fs.Path
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructField
import ai.cookie.spark.sql.sources.DataSourceTest
import org.scalatest.{BeforeAndAfter, Matchers}

class IrisRelationSuite extends DataSourceTest with SharedSQLContext
  with BeforeAndAfter with Matchers {

  val path = new Path("src/test/resources/iris.libsvm")
  val numExamples = 300
  
  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  test("metadata") {
    val df = sqlContext.read.iris(path.toString)

    def values(field: StructField) = {
      Attribute.fromStructField(field) match {
        case na: NominalAttribute => na.values
        case _ => None
      }
    }

    values(df.schema("label")) should equal (Some(IrisRelation.labels))
  }

  test("select") {
    val df = sqlContext.read.iris(path.toString)

    //df.rdd.partitions.length shouldEqual 10
    df.count() shouldEqual numExamples
    df.select("label").count() shouldEqual numExamples
    df.select("features").count() shouldEqual numExamples
    df.sample(false, 0.05).show()
  }

  test("sql datasource") {
    sqlContext.sql(
      s"""
         |CREATE TEMPORARY TABLE iris
         |USING ai.cookie.spark.sql.sources.iris
         |OPTIONS (path "$path")
      """.stripMargin)

    val df = sqlContext.sql("SELECT * FROM iris")
    df.count() shouldEqual numExamples
    df.sample(false, 0.05).show()
  }

  test("repeatability") {
    val df =  sqlContext.read.iris(path.toString)

    df.count() shouldEqual numExamples
    df.count() shouldEqual numExamples
  }

}

//class SaveLoadSuite extends DataSourceTest with SharedSQLContext with BeforeAndAfter {
//
//}