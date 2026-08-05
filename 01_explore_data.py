from pyspark.sql import SparkSession

# Create a Spark session, this will be the entry point to everything Spark will do
spark = SparkSession.builder \
.appName("IoT Sensor Data Exploration") \
.getOrCreate()

# Load the CSV
df = spark.read.csv("sensor_data.csv", header =  True, inferSchema = True)

# Show the schema (column names and data types Spark detected)
df.printSchema()

# Show the first 10 rows, similar to SQL's "SELECT * FROM orders LIMIT 10"
df.show(10)

# Show the total row count
print(f"Total rows: {df.count()}")