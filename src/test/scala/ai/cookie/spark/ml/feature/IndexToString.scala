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

package ai.cookie.spark.ml.feature

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StringType}

/**
  * A transformer that maps an ML column of label indices to
  * a string column of corresponding labels.
  *
  * Similar to the built-in IndexToString transformer in Spark 1.5.
  *
  * @see [StringIndexer] for the inverse transformation
  */
class IndexToString(override val uid: String)
  extends UnaryTransformer[Double,String,IndexToString] {

  def this() = this(Identifiable.randomUID("nominalLabel"))

  override protected def createTransformFunc: Double => String = {
    throw new UnsupportedOperationException();
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val schema = transformSchema(dataset.schema, logging = true)

    val values = Attribute.fromStructField(schema($(inputCol))) match {
      case na: NominalAttribute if na.values.isDefined => na.values.get
      case _ => throw new UnsupportedOperationException("input column must be a nominal column")
    }

    val toStringUdf = udf((index: Double) => values(index.toInt))
    dataset.withColumn($(outputCol), toStringUdf(dataset($(inputCol))))
  }

  override protected def outputDataType: DataType = StringType
}