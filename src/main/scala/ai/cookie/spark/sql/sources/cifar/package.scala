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

import org.apache.spark.sql.{DataFrame, DataFrameReader}
import ai.cookie.spark.sql.sources.cifar.DefaultSource._

package object cifar {

  /**
    * Extends [[org.apache.spark.sql.DataFrameReader]] with CIFAR support.
    * @param read the associated reader.
    */
  implicit class CifarDataFrameReader(read: DataFrameReader) {

    /**
      * Read a CIFAR-10 or CIFAR-100 dataset.
      *
      * @param path the Hadoop-style path to a CIFAR data file in the given format.
      * @param format the format, either 'CIFAR-10' or 'CIFAR-100' (binary variant).
      * @param maxSplitSize the maximum size (in bytes) of data per partition.  Default: 10MB.
      */
    def cifar(path: String, format: String, maxSplitSize: Option[Long] = None): DataFrame = {
      val parameters = (
        Seq("path" -> path, DefaultSource.Format -> format) ++ maxSplitSize.map(MaxSplitSize -> _.toString)
        ).toMap

      read.format(classOf[DefaultSource].getName).options(parameters).load()
    }
  }
}
