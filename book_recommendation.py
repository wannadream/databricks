# Databricks notebook source
booksFilename="dbfs:/mnt/files/validated/books.csv"
ratingsFilename="dbfs:/mnt/files/validated/ratings.csv"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/files/

# COMMAND ----------

# MAGIC %md
# MAGIC ###Analyzing the metadata

# COMMAND ----------

from pyspark.sql.types import *

booksDfSchema = StructType([
    StructField("BookId", IntegerType),
    StructField("Title", StringType)
])

booksWithGenresDfSchema = StructType([
    StructField("BookId", IntegerType),
    StructField("Title", StringType),
    StructField("Genre", StringType)
])

booksDataframes = sqlContext.read.format("com.databricks.spark.csv").options(
    header=True, inferSchema=False
).schema(booksDfSchema).load(booksFilename)

booksGenresDataframes = sqlContext.read.format("com.databricks.spark.csv").options(
    header=True, inferSchema=False
).schema(booksWithGenresDfSchema).load(booksFilename)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Checking DataFrames

# COMMAND ----------

booksDfSchema.show(4, truncate=False)
booksWithGenresDfSchema.show(4, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Buil-in functions for some insights

# COMMAND ----------

display(booksWithGenresDfSchema.groupBy("Genre").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Book ratings

# COMMAND ----------

ratingsDfSchema = StructType([
    StructField("UserId", IntegerType),
    StructField("BookId", IntegerType),
    StructField("Rating", IntegerType)
])

ratingsDataFrames = sqlContext.read.format("com.databricks.spark.csv").options(
    header=True, inferSchema=False
).schema(ratingsDfSchema).load(ratingsFilename)
ratingsDataFrames.show(4)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Caching data

# COMMAND ----------

booksDataframes.cache()
ratingsDataFrames.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Aggregation and join

# COMMAND ----------

from pyspark.sql import functions as F

bookIdsWithAvgRatingsDataFrames = ratingsDataFrames.groupBy("BookId").agg(F.count(ratingsDataFrames.Rating).alias("count"), F.avg(ratingsDataFrames.Rating).alias("average"))
bookIdsWithAvgRatingsDataFrames.show(4, truncate=False)

bookNamesWithAvgRatingsDataFrames = bookIdsWithAvgRatingsDataFrames.join(booksDataframes, "BookId")
bookNamesWithAvgRatingsDataFrames.show(4, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Machine Learning

# COMMAND ----------

# Training 60%, validation 20%, testing 20%
seed = 4
(split60DataFrames, split20aDataFrames, split20bDataFrames) = ratingsDataFrames.randomSplit([0.6,0.2,0.2], seed)

# Cache the data
trainingDataFrames = split60DataFrames.cache()
validationDataFrames = split20aDataFrames.cache()
testingDataFrames = split20bDataFrames.cache()

# Alternating Least Square https://sophwats.github.io/2018-04-05-gentle-als.html
from pyspark.ml.recommendation import ALS
als = ALS()

als.setPredictionCol("Prediction")\
   .setMaxIter(5)\
   .setSeed(seed)\
   .setRegParam(0.1)\
   .setUserCol("userId")\
   .setItemCol("BookId")\
   .setRatingCol("Rating")\
   .setRank(8)
ratingsModel = als.fit(trainingDataFrames)

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col

# Create RMSE(https://en.wikipedia.org/wiki/Root-mean-square_deviation) evaluator using label and predicted columns
regEval = RegressionEvaluator(predictionCol="Prediction", labelCol="Rating", metricName="rmse")
predictDataFrames = ratingsModel.transform(testingDataFrames);
# Remove NaN values
predictedTestRatingsDataFrames = predictDataFrames.filter(predictDataFrames.Prediction != float('nan'))
# Run evaluator on predictedTestRatingsDataFrames
testRMSERatings = regEval.evaluate(predictedTestRatingsDataFrames)
print("The model had a RSME on the test set of {0}".format(testRMSERatings))
# Get input of User ID and return the recommendation
dbutils.widgets.text("userId", "5", "")
value = dbutils.widgets.get("userId")
userId = int(value)
learningResult = predictedTestRatingsDataFrames.filter(col("UserID") == userId)
bookRecommendation = learningResult.join(booksDataframes, "BookId").select("Title").take(10)
dbutils.notebook.exit(bookRecommendation)

# COMMAND ----------


