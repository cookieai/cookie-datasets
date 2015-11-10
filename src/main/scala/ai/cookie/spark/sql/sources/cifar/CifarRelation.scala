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

import java.net.URI
import java.nio.file.Paths

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.{Experimental, AlphaComponent}
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.{SparkContext, Partition, TaskContext, Logging}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{PrunedScan, BaseRelation, RelationProvider}
import ai.cookie.spark.ml.attribute.AttributeKeys
import ai.cookie.spark.sql.sources.mapreduce.PrunedReader
import ai.cookie.spark.sql.types.VectorUDT

/**
 * CIFAR dataset as a Spark SQL relation.
 *
 */
private case class Cifar10Relation(val path: Path, val maxSplitSize: Option[Long] = None)
                          (override val sqlContext: SQLContext)
  extends CifarRelation(path, maxSplitSize)(sqlContext) {

  private lazy val labelMetadata = NominalAttribute.defaultAttr
    .withName("label").withValues(CifarFormats._10.labels).toMetadata()

  override def schema: StructType = StructType(
      StructField("label", DoubleType, nullable = false, labelMetadata) ::
      StructField("features", VectorUDT(), nullable = false, featureMetadata) :: Nil)

  CifarRecordReader.setFormat(hadoopConf, "CIFAR-10")
}

/**
 * CIFAR dataset as a Spark SQL relation.
 *
 */
private case class Cifar100Relation(val path: Path, val maxSplitSize: Option[Long] = None)
    (override val sqlContext: SQLContext)
  extends CifarRelation(path, maxSplitSize)(sqlContext) {

  private lazy val coarseLabelMetadata = NominalAttribute.defaultAttr
    .withName("coarseLabel").withValues(CifarFormats._100.coarseLabels).toMetadata()

  private lazy val labelMetadata = NominalAttribute.defaultAttr
    .withName("label").withValues(CifarFormats._100.fineLabels).toMetadata()

  override def schema: StructType = StructType(
    StructField("coarseLabel", DoubleType, nullable = false, coarseLabelMetadata) ::
      StructField("label", DoubleType, nullable = false, labelMetadata) ::
      StructField("features", VectorUDT(), nullable = false, featureMetadata) :: Nil)

  CifarRecordReader.setFormat(hadoopConf, "CIFAR-100")
}

private abstract class CifarRelation(
    path: Path,
    maxSplitSize: Option[Long] = None)
    (val sqlContext: SQLContext)
  extends BaseRelation with PrunedScan with Logging  {

  protected val hadoopConf = new Configuration(sqlContext.sparkContext.hadoopConfiguration)

  protected lazy val featureMetadata = {
    val shape = CifarFormats.SHAPE
    new MetadataBuilder().putLongArray(AttributeKeys.SHAPE, Array(shape._1, shape._2, shape._3)).build()
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val sc = sqlContext.sparkContext

    PrunedReader.setRequiredColumns(hadoopConf, requiredColumns)

    hadoopConf.setLong("mapreduce.input.fileinputformat.split.maxsize", maxSplitSize.getOrElse(10 * 1024 * 1024))

    val baseRdd = sc.newAPIHadoopFile[String, Row, CifarInputFormat](
      path.toString, classOf[CifarInputFormat], classOf[String], classOf[Row], hadoopConf)

    baseRdd.map { case (key, value) => value }
  }
}

/**
 * CIFAR data source.
 */
class DefaultSource extends RelationProvider {
  import DefaultSource._

  override def createRelation(sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val path = new Path(parameters.getOrElse("path", sys.error("path must be specified to CIFAR dataset")))
    val maxSplitSize = parameters.get(MaxSplitSize).map(_.toLong)

    parameters.get(DefaultSource.Format).map { format =>
      format match {
        case "CIFAR-10" => new Cifar10Relation(path, maxSplitSize)(sqlContext)
        case "CIFAR-100" => new Cifar100Relation(path, maxSplitSize)(sqlContext)
        case _ => sys.error(s"unrecognized format '$format'")
      }
    } getOrElse sys.error("format must be specified for CIFAR dataset")
  }
}

/**
  * Companion object for CIFAR data source, with option keys.
  */
object DefaultSource {
  /**
    * The maximum partition size (in bytes).
    */
  val MaxSplitSize = "maxSplitSize"

  /**
    * A CIFAR format specifier, with values 'CIFAR-10' and 'CIFAR-100'.
    */
  val Format = "format"
}


