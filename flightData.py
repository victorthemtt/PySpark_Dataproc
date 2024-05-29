#!/usr/bin/env python
# coding: utf-8

# ### 1. Setting up Spark

# In[5]:


from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("Flight Data Analysis") \
    .getOrCreate()



# In[7]:


flightData2015 = spark\
    .read\
    .option("inferSchema", "true")\
    .option("header", "true")\
    .csv("gs://mttspark/2015-summary.csv")


# In[8]:


flightData2015.take(3)


# In[9]:


flightData2015.sort("count").explain()


# In[10]:


spark.conf.set("spark.sql.shuffle.partitions", "5")
flightData2015.sort("count").take(2)


# ### DataFrames and SQL

# In[11]:


flightData2015.createOrReplaceTempView("flight_data_2015")


# In[12]:


sqlWay = spark.sql("""
    SELECT DEST_COUNTRY_NAME, count(1)
    FROM flight_data_2015
    GROUP BY DEST_COUNTRY_NAME
""")
dataFrameWay = flightData2015.groupBy("DEST_COUNTRY_NAME").count()

sqlWay.explain()
dataFrameWay.explain()


# In[13]:


spark.sql("SELECT max(count) from flight_data_2015").take(1)


# In[14]:


from pyspark.sql.functions import max
flightData2015.select(max("count")).take(1)


# In[15]:


maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")
maxSql.show()


# In[16]:


from pyspark.sql.functions import desc

flightData2015\
.groupBy("DEST_COUNTRY_NAME")\
.sum("count")\
.withColumnRenamed("sum(count)", "destination_total")\
.sort(desc("destination_total"))\
.limit(5)\
.explain()


# ### 2. Climate Change: Project Tasmania

# In[18]:


CO2 = spark.read.csv("gs://mttspark/CO2 emissions per capita per country.csv", inferSchema = True, header = True)

Temperature = spark.read.csv("gs://mttspark/GlobalLandTemperatures_GlobalLandTemperaturesByCountry.csv", inferSchema = True, header = True)


# In[19]:


CO2.show(5)


# In[20]:


Temperature.show(5)


# On Canvas, you will find two datasets. The first dataset contains temperature data by countries. Date starts from 1750 for average land temperature and goes up to 2015. Answer the following questions:
#
# a. For which country and during what year, the highest average temperature
# was observed? [5 pts]

# In[21]:


# Solve this question in the SQL way
Temperature.createOrReplaceTempView("tempData")

query = """
SELECT Country, year(dt) as year, AverageTemperature
FROM tempData
WHERE AverageTemperature = (SELECT MAX(AverageTemperature) FROM tempData)
"""

result = spark.sql(query)
result.show()


# b. Analyze the data by country over the years, and name which are the top
# 10 countries with the biggest change in average temperature. [5 pts]

# In[22]:


# Use SQL query to calculate variance and find the top 10 countries with the highest variance
query = """
SELECT country, VAR_SAMP(AverageTemperature) AS temp_variance
FROM tempData
GROUP BY country
ORDER BY temp_variance DESC
LIMIT 10
"""

result = spark.sql(query)
result.show()


# The second dataset contains data on CO2 Emissions per capita across countries
# from 1960 to 2014.
#
# a. Merge the two datasets by country, and keep the data from 1960 to 2014.
# [10 pts]

# In[23]:


from pyspark.sql.functions import year, avg

TEMP = Temperature.drop("AverageTemperatureUncertainty")

# Extract year from dt and calculate average temperature per country per year
TEMP = TEMP.withColumn("Year", year("dt"))
TEMP = TEMP.groupBy("Country", "Year").agg(avg("AverageTemperature").alias("AvgTemp"))

# Filter data to include only the years from 1960 to 2014
TEMP = TEMP.filter((TEMP.Year >= 1960) & (TEMP.Year <= 2014))

# Pivot the data to have years as columns and countries as rows
TEMP = TEMP.groupBy("Country").pivot("Year").avg("AvgTemp")

# Add a prefix to avoid ambiguity
from pyspark.sql.functions import col
year_columns = [c for c in TEMP.columns if c != 'Country']

# Now, rename each year column by adding 'temp_' prefix
for year in year_columns:
    TEMP = TEMP.withColumnRenamed(year, 'temp_' + year)

TEMP.show(5)


# In[24]:


CO2.show(5)


# In[25]:


## Merge the two datasets by country

joinedData = TEMP.join(CO2, TEMP.Country == CO2["Country Name"], "inner")
joinedData = joinedData.drop(CO2["Country Name"])

# Show the top 5 rows to inspect the result
joinedData.show(5)


# In[26]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, corr, lit
from functools import reduce
from pyspark.sql import DataFrame

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Environmental Data Analysis") \
    .getOrCreate()

# Example of converting data to a long format for correlation analysis
years = list(range(1960, 2014))
longFormatData = []

for year in years:
    yearData = joinedData.select(
        "Country",
        col(f"temp_{year}").alias("Temperature"),
        col(f"{year}").alias("CO2")
    ).withColumn("Year", lit(str(year)))
    longFormatData.append(yearData)

finalData = reduce(DataFrame.unionAll, longFormatData)

# Calculate correlation between temperature and CO2 emission
correlation = finalData.select(corr("Temperature", "CO2").alias("Correlation"))
correlation.show()


# Reflect on your everyday life activities. What can you personally do to make a
# positive change for the environment? Write a short paragraph with your thoughts.

# Personally, I can
# 1. Reduce travel by car, increase travel by public transportation to decrease CO2 emission.
#
# 2. Book flights that's more environment-friendly that emit less CO2.
#
# 3. Protect environment by sorting my garbage properly.
#

# ### 3. Start thinking about your final project
#
# ● Ideate 3 ideas about new sources of information that can underpin new
# companies, and type a short paragraph describing your ideas (bullet points are
# accepted). [10 pts]

# Here is my two cents:
#
# 1. Big Data from Low Earth Orbit (LEO) Satellite Systems:
# Low Earth orbit satellites provide real-time video and imaging data, offering a basis for businesses in real-time monitoring and environmental tracking. This new source of massive, accessible data could support numerous startups in diverse sectors.
#
# 2. Internet of Things (IoT) and Smart Devices Data:
# IoT devices continuously generate data that can be utilized for services like home automation or energy management. This vast amount of information from everyday smart devices opens avenues for innovations in consumer services and security systems.
#
# 3. Blockchain and Decentralized Data Systems:
# Blockchain technology offers a secure and decentralized ledger of transactions, ideal for solutions in supply chain transparency and secure financial dealings. This technology could foster new businesses that prioritize data security and transparency without intermediaries.
