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

import java.net.URI
import java.nio.file.Paths

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.{Experimental, AlphaComponent}
import org.apache.spark.{SparkContext, Partition, TaskContext, Logging}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{PrunedScan, BaseRelation, RelationProvider}
import ai.cookie.spark.ml.attribute.AttributeKeys
import ai.cookie.spark.sql.sources.mapreduce.PrunedReader
import ai.cookie.spark.sql.types.VectorUDT
import org.apache.spark.sql.types.MetadataBuilder

/**
  * MNIST dataset as a Spark SQL relation.
  */
private case class MnistRelation(
                          imagesPath: Path,
                          labelsPath: Path,
                          maxRecords: Option[Int] = None,
                          maxSplitSize: Option[Long] = None)
    (@transient val sqlContext: SQLContext) extends BaseRelation
    with PrunedScan with Logging  {

  private val hadoopConf = new Configuration(sqlContext.sparkContext.hadoopConfiguration)

  private lazy val shape = {
    var parser = new MnistImageReader(imagesPath)(hadoopConf)
    val s = (parser.numRows, parser.numColumns)
    parser.close()
    s
  }

  private lazy val featureMetadata = {
    new MetadataBuilder().putLongArray(AttributeKeys.SHAPE, Array(shape._1, shape._2)).build()
  }

  override def schema: StructType = StructType(
    StructField("label", DoubleType, nullable = false) ::
      StructField("features", VectorUDT(), nullable = false, featureMetadata) :: Nil)

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val sc = sqlContext.sparkContext

    PrunedReader.setRequiredColumns(hadoopConf, requiredColumns)

    hadoopConf.setLong("mapreduce.input.fileinputformat.split.maxsize", maxSplitSize.getOrElse(10 * 1024 * 1024))
    hadoopConf.set("ai.cookie.spark.sql.sources.mnist.labelsPath", labelsPath.toString)

    val baseRdd = sc.newAPIHadoopFile[String, Row, MnistInputFormat](
      imagesPath.toString, classOf[MnistInputFormat], classOf[String], classOf[Row], hadoopConf)

    baseRdd.map { case (key, value) => value }
  }
}

/**
 * MNIST data source.
 */
class DefaultSource extends RelationProvider {
  import DefaultSource._

  private def checkImagesFilePath(parameters: Map[String, String]): String = {
    parameters.getOrElse(ImagesPath,
      sys.error("'imagesPath' must be specified for MNIST images"))
  }

  private def checkLabelsFilePath(parameters: Map[String, String]): String = {
    parameters.getOrElse(LabelsPath,
      sys.error("'labelsPath' must be specified for MNIST labels"))
  }

  private def checkMaxRecords(parameters: Map[String, String]): Option[Int] = {
    parameters.get(MaxRecords).map(_.toInt)
  }

  private def checkMaxSplitSize(parameters: Map[String, String]): Option[Long] = {
    parameters.get(MaxSplitSize).map(_.toLong)
  }

  override def createRelation(sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val imagesPath = new Path(checkImagesFilePath(parameters))
    val labelsPath = new Path(checkLabelsFilePath(parameters))
    val maxRecords = checkMaxRecords(parameters)
    val maxSplitSize = checkMaxSplitSize(parameters)
    new MnistRelation(imagesPath, labelsPath, maxRecords, maxSplitSize)(sqlContext)
  }
}

/**
  * Companion object for MNIST data source, with option keys.
  */
object DefaultSource {
  val ImagesPath = "imagesPath"
  val LabelsPath = "labelsPath"
  val MaxRecords = "maxRecords"
  val MaxSplitSize = "maxSplitSize"
}


