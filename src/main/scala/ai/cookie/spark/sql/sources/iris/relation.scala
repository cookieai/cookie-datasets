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

import ai.cookie.spark.sql.types.VectorUDT
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NominalAttribute, NumericAttribute}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import ai.cookie.spark.sql.sources.libsvm.LibSVMRelation
import org.apache.spark.ml
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType

/**
  * Iris dataset as a Spark SQL relation.
  *
  * Implements the LibSVM variant.
  */
private class IrisLibSVMRelation(override val path: String)
    (@transient override val sqlContext: SQLContext)
  extends LibSVMRelation(path, Some(IrisRelation.features.length))(sqlContext)
   {
  protected override def labelMetadata = NominalAttribute.defaultAttr
    .withName("label").withValues(IrisRelation.labels).toMetadata()

  protected override def featuresMetadata = new AttributeGroup("features",
    IrisRelation.features.map(NumericAttribute.defaultAttr.withName(_): Attribute))
    .toMetadata()
}

/**
  * Iris dataset as a Spark SQL relation.
  *
  * Implements the CSV variant.
  */
private class IrisCsvRelation(val path: String)
   (@transient override val sqlContext: SQLContext)
  extends BaseRelation with TableScan with IrisRelation {

  override def schema: StructType = StructType(
    StructField("label", DoubleType, nullable = false, metadata = labelMetadata) ::
      StructField("features", VectorType, nullable = false, metadata = featuresMetadata) :: Nil)

  override def buildScan(): RDD[Row] = {
    val sc = sqlContext.sparkContext

    sc.textFile(path).filter(_.length != 0).map {
      _.split(",") match {
        case a: Array[String] if a.length == 5 => Row(
          // label
          a(4) match {
            case "Iris-setosa" => 0.0
            case "Iris-versicolor" => 1.0
            case "Iris-virginica" => 2.0
          },
          // features
          ml.linalg.Vectors.dense(a.slice(0,4).map(_.toDouble)))
        case _ => sys.error("unrecognized format")
      }
    }
  }
}

private trait IrisRelation {
  protected def labelMetadata = NominalAttribute.defaultAttr
    .withName("label").withValues(IrisRelation.labels).toMetadata()

  protected def featuresMetadata = new AttributeGroup("features",
    IrisRelation.features.map(NumericAttribute.defaultAttr.withName(_): Attribute))
    .toMetadata()
}

private object IrisRelation {
  /**
   * The indexed string labels corresponding to the dataset.
   */
  val labels = Array("I. setosa", "I. versicolor", "I. virginica")

  /**
   * The features or attributes of each element.
   */
  val features = Array("sepal length (cm)", "sepal width (cm)", "petal length (cm)", "petal width (cm)")
}

/**
 * IRIS data source.
 */
class DefaultSource extends RelationProvider {
  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified for Iris dataset in LIBSVM format."))
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val path = checkPath(parameters)

    parameters.get(DefaultSource.Format).map { format =>
      format match {
        case "csv" => new IrisCsvRelation(path)(sqlContext)
        case "libsvm" => new IrisLibSVMRelation(path)(sqlContext)
        case _ => sys.error(s"unrecognized format '$format'")
      }
    } getOrElse new IrisCsvRelation(path)(sqlContext)
  }
}

object DefaultSource {
  /**
    * The format (csv or libsvm)
    */
  val Format = "format"
}