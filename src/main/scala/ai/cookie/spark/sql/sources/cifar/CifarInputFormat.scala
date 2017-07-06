
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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext, InputSplit => HadoopInputSplit, RecordReader => HadoopRecordReader}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import ai.cookie.spark.sql.sources.mapreduce.PrunedReader
import ai.cookie.spark.sql.types.Conversions._
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.spark.mllib.util.MLUtils

private class CifarInputFormat
  extends FileInputFormat[String, Row]
{
  override protected def isSplitable(context: JobContext, file: Path): Boolean = true

  override def createRecordReader(split: HadoopInputSplit, context: TaskAttemptContext)
  : HadoopRecordReader[String, Row] = {
    new CifarRecordReader()
  }
}

private class CifarRecordReader()
  extends HadoopRecordReader[String,Row] {

  private var parser: CifarReader = null

  private var length = 0
  private var remaining = 0

  private var cols: Array[CifarRecord => Any] = null
  private var currentValue: Row = null

  override def initialize(split: HadoopInputSplit, context: TaskAttemptContext): Unit = {
    implicit var conf = context.getConfiguration
    val file = split.asInstanceOf[FileSplit]
    val requiredColumns = PrunedReader.getRequiredColumns(context.getConfiguration)

    // initialize parser
    val format = {
      CifarRecordReader.getFormat(context.getConfiguration) match {
        case Some("CIFAR-10") => CifarFormats.Cifar10
        case Some("CIFAR-100") => CifarFormats.Cifar100
        case other => throw new RuntimeException(s"unsupported CIFAR format '$other'")
      }
    }
    parser = new CifarReader(file.getPath, format, requiredColumns.contains("features"))

    // calculate the range of records to scan, based on byte-level split information
    val start = parser.recordAt(file.getStart)
    length = parser.recordAt(file.getStart + split.getLength) - start
    remaining = length
    parser.seek(start)

    // construct column generators
    cols = requiredColumns.flatMap {
      case "coarseLabel" => Seq((record:CifarRecord) => record.coarseLabel.toDouble)
      case "label" => Seq((record:CifarRecord) => record.fineLabel.toDouble)
      case "features" => Seq((record:CifarRecord) => record.image: Vector)
    }
  }

  override def getProgress: Float = 1 - Math.ceil(remaining / length.toDouble).toFloat

  override def nextKeyValue(): Boolean = {
    remaining match {
      case 0 => false
      case _ =>
        val record = parser.next()
        currentValue = Row.fromSeq(cols.map(_(record)))
        remaining -= 1
        true
    }
  }

  override def getCurrentKey: String = ""

  override def getCurrentValue: Row = currentValue

  override def close(): Unit = {
    Option(parser).foreach(p => p.close())
  }
}

private object CifarRecordReader {
  val FORMAT = "ai.cookie.spark.sql.sources.cifar.FORMAT"

  def setFormat(conf: HadoopConfiguration, format: String): Unit = {
    conf.set(FORMAT, format)
  }

  def getFormat(conf: HadoopConfiguration): Option[String] = {
    Option(conf.get(FORMAT))
  }
}
