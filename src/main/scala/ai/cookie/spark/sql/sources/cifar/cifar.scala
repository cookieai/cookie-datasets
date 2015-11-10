
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

import java.io.{Closeable, IOException}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * Read the CIFAR (binary) format.  Both the CIFAR-10 and CIFAR-100 variants are supported.
 *
 * http://www.cs.toronto.edu/~kriz/cifar.html
 */
private class CifarReader
  (path: Path, format: CifarFormats.Format, readImage: Boolean)
  (implicit conf: Configuration)
  extends Closeable with Iterator[CifarRecord]
{
  private val (summary,stream) = {
    val fs = path.getFileSystem(conf)
    val summary = fs.getContentSummary(path)
    if(summary.getLength % format.recordSize != 0) throw new IOException("invalid CIFAR input")
    (summary, fs.open(path))
  }

  private val buffer = new Array[Byte](CifarFormats.IMAGE_FIELD_SIZE)

  def seek(n: Int): Unit = stream.seek(n * format.recordSize)

  def hasNext: Boolean = stream.getPos + format.recordSize <= summary.getLength

  /**
   * Read the next record.
   * @return a record object.   Note that the buffer is reused.
   */
  def next(): CifarRecord = {
    val row = CifarRecord(
      coarseLabel = format match {
        case CifarFormats._100 => stream.readUnsignedByte()
        case CifarFormats._10 => 0
      },
      fineLabel = stream.readUnsignedByte(),
      image = readImage match {
        case true =>
          assert(buffer.length == CifarFormats.IMAGE_FIELD_SIZE)
          stream.readFully(buffer)
          buffer
        case _ =>
          stream.skip(CifarFormats.IMAGE_FIELD_SIZE)
          null
      }
    )
    row
  }

  def skip(): Unit = {
    stream.skip(format.recordSize)
  }

  def close(): Unit = {
    stream.close()
  }

  /**
   * Returns the index of the row at the given position.
   * @param pos the byte-wise position in the stream
   * @return the record index
   */
  def recordAt(pos: Long): Int = {
    Math.ceil(pos / format.recordSize.toFloat).toInt
  }
}

private case class CifarRecord(
  coarseLabel: Int,
  fineLabel: Int,
  image: Array[Byte]) {
}

private object CifarFormats {
  val SHAPE = (3,32,32)
  val IMAGE_FIELD_SIZE = SHAPE._1 * SHAPE._2 * SHAPE._3

  private def read(resourceName: String): Array[String] = {
    val stream = getClass.getResourceAsStream(resourceName)
    try {
      scala.io.Source.fromInputStream(stream).getLines().filterNot(_.isEmpty).toArray
    }
    finally {
      Option(stream).map { _.close() }
    }
  }

  sealed abstract class Format(val name: String, val recordSize: Int)

  case object _10 extends Format("CIFAR-10", 1 + IMAGE_FIELD_SIZE) {
    val labels = read("batches.meta.txt")
  }

  case object _100 extends Format("CIFAR-100", 1 + 1 + IMAGE_FIELD_SIZE) {
    val fineLabels = read("fine_label_names.txt")
    val coarseLabels = read("coarse_label_names.txt")
  }
}