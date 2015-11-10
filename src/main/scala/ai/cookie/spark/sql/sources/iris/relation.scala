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

package ai.cookie.spark.sql.sources.iris

import org.apache.spark.ml.attribute.{Attribute, NumericAttribute, AttributeGroup, NominalAttribute}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import ai.cookie.spark.sql.sources.libsvm.LibSVMRelation

/**
 * Iris dataset as a Spark SQL relation.
 *
 */
private class IrisRelation(override val path: String)
    (@transient override val sqlContext: SQLContext)
  extends LibSVMRelation(path, Some(IrisRelation.features.length))(sqlContext) {

  override protected def labelMetadata = NominalAttribute.defaultAttr
    .withName("label").withValues(IrisRelation.labels).toMetadata()

  override protected def featuresMetadata = new AttributeGroup("features",
    IrisRelation.features.map(NumericAttribute.defaultAttr.withName(_): Attribute))
    .toMetadata()
}

private object IrisRelation {
  /**
   * The indexed string labels corresponding to the dataset.
   */
  val labels = Array("I. setosa", "I. versicolor", "I. virginica")

  /**
   * The features or attributes of each element.
   */
  val features = Array("sepal length (cm)", "sepal width (cm)", "petal length (cm)", "petal width (cm)")
}

/**
 * IRIS data source.
 */
class DefaultSource extends RelationProvider {
  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified for Iris dataset in LIBSVM format."))
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val path = checkPath(parameters)
    new IrisRelation(path)(sqlContext)
  }
}
