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

package object iris {

  /**
    * Extends [[org.apache.spark.sql.DataFrameReader]] with IRIS support.
    * @param read the associated reader.
    */
  implicit class IrisDataFrameReader(read: DataFrameReader) {

    /**
      * Read an IRIS dataset.
      * @param path a Hadoop-style path to a file in IRIS format.
      */
    def iris(path: String): DataFrame =
      read.format(classOf[DefaultSource].getName).load(path)
  }
}


