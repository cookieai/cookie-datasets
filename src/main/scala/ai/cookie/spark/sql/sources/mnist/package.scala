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

package ai.cookie.spark.sql.sources

import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}
import ai.cookie.spark.sql.sources.mnist.DefaultSource._

package object mnist {

  /**
    * Extends [[org.apache.spark.sql.DataFrameReader]] with MNIST support.
    * @param read the associated reader.
    */
  implicit class MnistDataFrameReader(read: DataFrameReader) {

    /**
      *
      * Read an MNIST dataset.
      * @param imagesPath the Hadoop-style path to the images data file.
      * @param labelsPath the Hadoop-style path to the labels data file.
      * @param maxSplitSize the maximum size (in bytes) of data per partition.  Default: 10MB.
      * @return
      */
    def mnist(imagesPath: String, labelsPath: String, maxSplitSize: Option[Long] = None): DataFrame = {
      val parameters = (
        Seq(ImagesPath -> imagesPath, LabelsPath -> labelsPath)
        ++ maxSplitSize.map(MaxSplitSize -> _.toString)
      ).toMap

      read.format(classOf[DefaultSource].getName).options(parameters).load()
    }
  }
}
