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
import ai.cookie.spark.sql.sources.libsvm.DefaultSource._

package object libsvm {

  /**
    * Extends [[org.apache.spark.sql.DataFrameReader]] with LIBSVM support.
    * @param read the associated reader.
    */
  implicit class LibSVMDataFrameReader(read: DataFrameReader) {

    /**
      * Read a LIBSVM dataset.
      * @param path the Hadoop-style path to a file in LIBSVM format.
      * @param numFeatures the number of features to expect per row.   If not specified, it is computed automatically.
      */
    def libsvm(path: String, numFeatures: Option[Int] = None): DataFrame = {
      val parameters = (
        Seq() ++ numFeatures.map(NumFeatures -> _.toString)
        ).toMap

      read.format(classOf[DefaultSource].getName).options(parameters).load(path)
    }
  }
}


