
println(
  """
    |------------------------------------------------
    |Setup
    |
  """.stripMargin)

import java.io.File
import java.net.URL
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.attribute.{AttributeGroup, NominalAttribute, Attribute}

import scala.sys.process._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc:SparkContext)
import sqlContext.implicits._
import sqlContext.sql

val cifar100Url = new URL("https://www.cs.toronto.edu/~kriz/cifar-100-binary.tar.gz")
val cacheDir = new File(System.getProperty("java.io.tmpdir"))

println("\nDownloading and extracting CIFAR-100 dataset...")
if(!new File(cacheDir, "cifar-100-binary").exists()) {
  cifar100Url #> s"tar -xz -C $cacheDir" !
}

val trainFile = new File(cacheDir, "cifar-100-binary/train.bin")
require(trainFile.exists())
val testFile = new File(cacheDir, "cifar-100-binary/test.bin")
require(testFile.exists())


println(
  """
    |------------------------------------------------
    |Demonstration: Working with read methods.
    |
    |The CIFAR-100 data source extends the DataFrame reader with a convenient 'cifar' method.
    |
  """.stripMargin)

import ai.cookie.spark.sql.sources.cifar._

println(s"\nLoading CIFAR-100 dataframe from ${trainFile.toURI}...")
val df1 = sqlContext.read.cifar(trainFile.toURI.toString, format = "CIFAR-100")
df1.sample(false, 0.1).show()


println(
  """
    |------------------------------------------------
    |Demonstration: Working with metadata.
    |
    |The CIFAR-100 schema contains useful column metadata to interoperate with Spark ML.
    |  - feature metadata indicating:
    |  -   a. the # of dimensions (i.e. # of elements in the feature vector).
    |  -   b. the image shape (i.e. # channels, height, width)
    |  - label metadata providing string labels for nominal values (1 -> "fish", etc).
    |
  """.stripMargin)
import org.apache.spark.ml.attribute.{NominalAttribute, Attribute, AttributeGroup}
import org.apache.spark.ml.feature.IndexToString

val dfM = sqlContext.read.cifar(trainFile.toURI.toString, format = "CIFAR-100")

println("\nPrinting metadata...")
Attribute.fromStructField(dfM.schema("coarseLabel"))
Attribute.fromStructField(dfM.schema("label"))
AttributeGroup.fromStructField(dfM.schema("features")).size
dfM.schema("features").metadata.getLongArray("shape")

println("\nTransforming dataframe to include a string label column based on in-built metadata...")
val pipe = new Pipeline().setStages(Array(
    new IndexToString().setInputCol("coarseLabel").setOutputCol("coarse"),
    new IndexToString().setInputCol("label").setOutputCol("fine")
  ))
pipe.fit(dfM).transform(dfM).select("coarse", "fine", "features").sample(false, 0.1).show


println(
  """
    |------------------------------------------------
    |Demonstration: Working with the Spark SQL language.
    |
    |Spark SQL provides a DDL to define a temporary table based on a data source.
    |
  """.stripMargin)

println(s"\nDefining the 'cifar100' table based on the downloaded file)...")
sql(
  s"""
     |CREATE TEMPORARY TABLE cifar100
     |USING ai.cookie.spark.sql.sources.cifar
     |OPTIONS (path "${trainFile.toURI}", format "CIFAR-100")
      """.stripMargin
)

println(s"\nLoading CIFAR-100 dataframe from 'cifar100' table (filtered for illustration purposes)...")
var df2 = sql("SELECT * FROM cifar100 where coarseLabel = 1") // filter on fish
df2.show


println(
  """
    |------------------------------------------------
    |Demonstration: Working with Spark ML.
    |
    |Train a machine learning model with CIFAR-100 data.
    |
    |Note that the classifier used here is not tuned for accuracy but for speed; expect bad results.
    |Use a convolutional neural network for better results.
    |
  """.stripMargin)

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

// load train/test datasets
val trainDF = sqlContext.read.cifar(trainFile.toURI.toString, format = "CIFAR-100")
val testDF = sqlContext.read.cifar(testFile.toURI.toString, format = "CIFAR-100")

// create an estimator
val trainer = {
  require(Attribute.fromStructField(trainDF.schema("label")).asInstanceOf[NominalAttribute].getNumValues == Some(100))
  require(AttributeGroup.fromStructField(trainDF.schema("features")).size == 3072)
  val layers = Array(3072, 100)

  new MultilayerPerceptronClassifier()
    .setLayers(layers)
    .setBlockSize(128)
    .setSeed(1234L)
    .setMaxIter(10)
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
