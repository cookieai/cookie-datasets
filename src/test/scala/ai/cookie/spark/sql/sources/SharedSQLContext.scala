
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

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql._
import org.scalatest.Suite

private[sql] abstract trait SharedSQLContext extends SharedSparkContext { self: Suite =>

  @transient private var _sqlContext: SQLContext = _

  def sqlContext: SQLContext = _sqlContext

  override def beforeAll() {
    super.beforeAll()
    _sqlContext = new SQLContext(sc)
  }

  override def afterAll() {
    _sqlContext = null
    super.afterAll()
  }
}