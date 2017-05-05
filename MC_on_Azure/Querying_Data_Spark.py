# Using spark on azure to Query a File Jupyter notebook 
# All Values appear to need to be Hard Coded
# This is for Call File 
from pyspark.sql.types import *   	# this is needed to run the queries 
									#if already in program run before the rest of the code below

# Load the data  ie the file wish to read
hvacText = sc.textFile("wasbs:///IBM_MC_Call_Result.csv")

# Create the schema  False means it can not contain a null value, StringType is that this value is of type String 
hvacSchema = StructType([StructField("DATE", StringType(), False),StructField("VARATION", StringType(), False),StructField("CALL_PRICE", StringType(), False),StructField("STRIKE_PRICE",StringType(), False)])

# Parse the data in hvacText, will split the data in the file on where the comma is 
hvac = hvacText.map(lambda s: s.split(",")).filter(lambda s: s[0] != "DATE").map(lambda s:(str(s[0]), str(s[1]), str(s[2]), str(s[3]) ))

# Create a data frame
hvacdf = sqlContext.createDataFrame(hvac,hvacSchema)

# Register the data fram as a table to run queries against
hvacdf.registerTempTable("callOption")

# for put files if in a diiferent program need from pyspark.sql.types import *

# Load the data
hvacText = sc.textFile("wasbs:///IBM_MC_Put_Result.csv")

# Create the schema
hvacSchema = StructType([StructField("DATE", StringType(), False),StructField("VARATION", StringType(), False),StructField("CALL_PRICE", StringType(), False),StructField("STRIKE_PRICE",StringType(), False)])

# Parse the data in hvacText
hvac = hvacText.map(lambda s: s.split(",")).filter(lambda s: s[0] != "DATE").map(lambda s:(str(s[0]), str(s[1]), str(s[2]), str(s[3]) ))

# Create a data frame
hvacdf = sqlContext.createDataFrame(hvac,hvacSchema)

# Register the data fram as a table to run queries against, its a temp table at the moment
hvacdf.registerTempTable("PutOption")