
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

package ai.cookie.spark.sql

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors

package object types {
  private[spark] object Conversions {

    /**
     * Convert a byte array to a Spark dense vector.
     * @param arr the array to convert.
     * @return a Spark vector.
     */
    implicit def toVector(arr: Array[Byte]): Vector = {
      val data = new Array[Double](arr.length)
      var i = 0
      while(i < arr.length) {
        data(i) = arr(i)
        i += 1
      }
      Vectors.dense(data)
    }
  }
}