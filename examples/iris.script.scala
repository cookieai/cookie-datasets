
println(
  """
    |------------------------------------------------
    |Setup
    |
  """.stripMargin)

import java.io.File
import java.net.URL
import scala.sys.process._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

val dataUrl = new URL("https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data")
val cacheFile = new File(System.getProperty("java.io.tmpdir"), "iris.data")

val sqlContext = new SQLContext(sc:SparkContext)
import sqlContext.implicits._
import sqlContext.sql

println("\nDownloading IRIS dataset...")
if(!cacheFile.exists()) dataUrl #> cacheFile !


println(
  """
    |------------------------------------------------
    |Demonstration: Working with read methods.
    |
    |The IRIS data source extends the DataFrame reader with a convenient 'iris' method.
    |
  """.stripMargin)

import ai.cookie.spark.sql.sources.iris._

println(s"\nLoading IRIS dataframe from ${cacheFile.toURI}...")
val df1 = sqlContext.read.iris(cacheFile.toURI.toString)
df1.sample(false, 0.1).show()


println(
  """
    |------------------------------------------------
    |Demonstration: Working with metadata.
    |
    |The IRIS schema contains useful column metadata to interoperate with Spark ML.
    |  - feature metadata labeling each dimension (0 -> "sepal length (cm)", etc).
    |  - label metadata providing string labels for nominal values (0 -> "I. setosa", etc).
    |
  """.stripMargin)
import org.apache.spark.ml.attribute.{NominalAttribute, Attribute, AttributeGroup}
import org.apache.spark.ml.feature.IndexToString

val dfM = sqlContext.read.iris(cacheFile.toURI.toString)

println("\nPrinting metadata...")
Attribute.fromStructField(dfM.schema("label"))
AttributeGroup.fromStructField(dfM.schema("features")).attributes.get

println("\nTransforming dataframe to include a string label column based on in-built metadata...")
val i2s = new IndexToString().setInputCol("label").setOutputCol("labelString")
i2s.transform(dfM).select("labelString", "features").sample(false, 0.1).show


println(
  """
    |------------------------------------------------
    |Demonstration: Working with the Spark SQL language.
    |
    |Spark SQL provides a DDL to define a temporary table based on a data source.
    |
  """.stripMargin)

println(s"\nDefining the 'iris' table based on the downloaded file)...")
sql(
s"""
   |CREATE TEMPORARY TABLE iris
   |USING ai.cookie.spark.sql.sources.iris
   |OPTIONS (path "${cacheFile.toURI}")
      """.stripMargin
)

println(s"\nLoading IRIS dataframe from 'iris' table (filtered for illustration purposes)...")
var df2 = sql("SELECT * FROM iris where label = 1")
df2.show


println(
  """
    |------------------------------------------------
    |Demonstration: Working with Spark ML.
    |
    |Train a neural network with IRIS data.
    |
  """.stripMargin)

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

// load train/test datasets
val mlDF = sqlContext.read.iris(cacheFile.toURI.toString)
val Array(trainDF, testDF) = mlDF.randomSplit(Array(0.6, 0.4), seed = 1234L)

val trainer = {
  // specify layers for the neural network:
  //   input layer of size 4 (features), two intermediate of size 5 and 4 and output of size 3 (classes)
  require(Attribute.fromStructField(mlDF.schema("label")).asInstanceOf[NominalAttribute].getNumValues == Some(3))
  require(AttributeGroup.fromStructField(mlDF.schema("features")).size == 4)
  val layers = Array(4, 5, 4, 3)

  new MultilayerPerceptronClassifier()
    .setLayers(layers)
    .setBlockSize(128)
    .setSeed(1234L)
    .setMaxIter(100)
    .setPredictionCol("label")
    .setPredictionCol("prediction")
}

// train the model
val model = trainer.fit(trainDF)

// test the model
val result = model.transform(testDF)
val predictionAndLabels = result.select("prediction", "label")
predictionAndLabels.sample(false, 0.33).show

// compute precision on the test set
val evaluator = {
  new MulticlassClassificationEvaluator()
    .setMetricName("precision")
}
val precision = evaluator.evaluate(predictionAndLabels)
println(f"Precision: ${precision*100}%1.2f%%")
