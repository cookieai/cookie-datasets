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

package ai.cookie.spark.sql.sources.libsvm

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, TableScan}
import org.apache.spark.sql.types.{DoubleType, Metadata, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import ai.cookie.spark.sql.types.VectorUDT
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType

/**
 * LibSVMRelation provides a DataFrame representation of LibSVM formatted data.
 * @param path the path to the data file.
 * @param numFeatures the number of features to expect per row.   If not specified, it is computed automatically.
 */
private[spark] class LibSVMRelation(val path: String, val numFeatures: Option[Int] = None)
    (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan {

  protected def labelMetadata: Metadata = Metadata.empty

  protected def featuresMetadata: Metadata = Metadata.empty

  override def schema: StructType = StructType(
    StructField("label", DoubleType, nullable = false, metadata = labelMetadata) ::
      StructField("features", VectorType, nullable = false, metadata = featuresMetadata) :: Nil)

  override def buildScan(): RDD[Row] = {

    val sc = sqlContext.sparkContext

    val baseRdd = numFeatures match {
      case Some(n) => MLUtils.loadLibSVMFile(sc, path, n)
      case None => MLUtils.loadLibSVMFile(sc, path)
    }

    baseRdd.map(pt => {
      Row(pt.label, pt.features.toDense.asML)
    })
  }

  override def hashCode(): Int = 41 * (41 + path.hashCode) + numFeatures.hashCode + schema.hashCode()

  override def equals(other: Any): Boolean = other match {
    case that: LibSVMRelation =>
      (this.path == that.path) && (this.numFeatures == that.numFeatures) && this.schema.equals(that.schema)
    case _ => false
  }
}

/**
  * LIBSVM data source.
  */
class DefaultSource extends RelationProvider {
  import DefaultSource._

  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified for LIBSVM dataset."))
  }

  private def checkNumFeatures(parameters: Map[String, String]): Option[Int] = {
    parameters.get(NumFeatures).map(_.toInt)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val path = checkPath(parameters)
    val numFeatures = checkNumFeatures(parameters)
    new LibSVMRelation(path, numFeatures)(sqlContext)
  }
}

/**
  * Companion object for LIBSVM data source, with option keys.
  */
object DefaultSource {
  /**
    * The number of features to expect per record.
    */
  val NumFeatures = "numFeatures"
}
