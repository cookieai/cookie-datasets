
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

import java.io.{IOException, Closeable}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}

private[mnist] class MnistLabelReader(path: Path)(implicit conf: Configuration)
  extends Closeable with Iterator[Int]
{
  private val stream: FSDataInputStream = {
    val fs = path.getFileSystem(conf)
    fs.open(path)
  }

  if (stream.readInt() != MnistLabelReader.HEADER_MAGIC) {
    throw new IOException("labels database file is unreadable")
  }

  val numLabels: Int = stream.readInt()

  def seek(n: Int): Unit = stream.seek(MnistLabelReader.HEADER_SIZE + n)

  override def hasNext: Boolean = stream.getPos < numLabels - MnistLabelReader.HEADER_SIZE

  override def next(): Int = stream.readUnsignedByte()

  def skip(): Unit = {
    stream.skip(1)
  }

  def close(): Unit = {
    stream.close()
  }

  def recordAt(pos: Long): Int = {
    (pos - MnistImageReader.HEADER_SIZE).toInt
  }
}

private object MnistLabelReader {
  val HEADER_SIZE = 8
  val HEADER_MAGIC = 0x00000801
}


private[mnist] class MnistImageReader(path: Path)(implicit conf: Configuration)
  extends Closeable with Iterator[Array[Byte]]
{
  private[mnist] val stream: FSDataInputStream = {
    val fs = path.getFileSystem(conf)
    fs.open(path)
  }

  if (stream.readInt() != MnistImageReader.HEADER_MAGIC) {
    throw new IOException("images database file is unreadable")
  }

  val numImages: Int = stream.readInt()

  val numRows: Int = stream.readInt()

  val numColumns: Int = stream.readInt()

  private val buffer = new Array[Byte](numRows * numColumns)

  def seek(n: Int): Unit = stream.seek(MnistImageReader.HEADER_SIZE + n * buffer.length)

  def hasNext: Boolean = stream.getPos < numImages * buffer.length - MnistImageReader.HEADER_SIZE

  def next(): Array[Byte] = {
    stream.readFully(buffer)
    buffer
  }

  def skip(): Unit = {
    stream.skip(buffer.length)
  }

  def close(): Unit = {
    stream.close()
  }

  def recordAt(pos: Long): Int = {
    Math.ceil((pos - MnistImageReader.HEADER_SIZE) / buffer.length.toFloat).toInt
  }
}

private object MnistImageReader {
  val HEADER_SIZE = 16
  val HEADER_MAGIC = 0x00000803
}
