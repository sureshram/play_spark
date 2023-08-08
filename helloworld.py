from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("HelloWorld") \
    .getOrCreate()

# Create a DataFrame with a single row containing "Hello, World!"
data = [("Hello, World!",)]
columns = ["message"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# Stop the Spark session
spark.stop()
