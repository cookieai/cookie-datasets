
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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.{InputSplit => HadoopInputSplit, JobContext, RecordReader => HadoopRecordReader, TaskAttemptContext}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import ai.cookie.spark.sql.sources.mapreduce.PrunedReader
import ai.cookie.spark.sql.types.Conversions._

private class MnistInputFormat
  extends FileInputFormat[String, Row]
{
  override protected def isSplitable(context: JobContext, file: Path): Boolean = true

  override def createRecordReader(split: HadoopInputSplit, context: TaskAttemptContext)
  : HadoopRecordReader[String, Row] = {
    new MnistRecordReader()
  }
}

private class MnistRecordReader()
  extends HadoopRecordReader[String,Row]
  {

  private var imageParser: MnistImageReader = null
  private var labelParser: MnistLabelReader = null

  private var length = 0
  private var remaining = 0

  private var cols: Array[() => Any] = null
  private var currentValue: Row = null

  override def initialize(split: HadoopInputSplit, context: TaskAttemptContext): Unit = {
    implicit var conf = context.getConfiguration
    val file = split.asInstanceOf[FileSplit]

    // initialize parsers
    val labelsPath = Option(conf.get("ai.cookie.spark.sql.sources.mnist.labelsPath"))
      .map(new Path(_))
      .getOrElse(throw new RuntimeException("expected labelsPath"))
    imageParser = new MnistImageReader(file.getPath)
    labelParser = new MnistLabelReader(labelsPath)
    // val recordRange = calculateRange(file, parser)

    // calculate the range of records to scan, based on byte-level split information
    val start = imageParser.recordAt(file.getStart)
    length = imageParser.recordAt(file.getStart + split.getLength) - start
    remaining = length
    imageParser.seek(start)
    labelParser.seek(start)

    // construct column generators
    val requiredColumns = PrunedReader.getRequiredColumns(context.getConfiguration)
    cols = requiredColumns.flatMap {
      case "label" => Seq(() => labelParser.next().toDouble)
      case "features" => Seq(() => imageParser.next(): Vector)
    }
  }

  override def getProgress: Float = 1 - Math.ceil(remaining / length.toDouble).toFloat

  override def nextKeyValue(): Boolean = {
    remaining match {
      case 0 => false
      case _ =>
        currentValue = Row.fromSeq(cols.map(_()))
        remaining -= 1
        true
    }
  }

  override def getCurrentKey: String = ""

  override def getCurrentValue: Row = currentValue

  override def close(): Unit = {
    Option(imageParser).foreach(p => p.close())
    Option(labelParser).foreach(p => p.close())
  }
}
