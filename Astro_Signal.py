import math
from pyspark.sql.functions import col

# Loading data
rdd = sc.textFile("pulsar.dat")

# Creating DataFrame
rdd_split = rdd.map(lambda x : x.split())
rdd_split_values = rdd_split.map(lambda x:[float(x[0]), float(x[1]), float(x[2]), float(x[3])])
rdd_df = spark.createDataFrame(rdd_split_values, ['ascension', 'declination', 'time', 'frequency'])
rdd_df.show()

# rounding function
def func_round(x):
    int_x = math.floor(x)
    val1 = int_x
    val2 = int_x + 0.5
    val3 = int_x + 1

    if x > val1 and x < val2:
        if (x - val1) < (val2 - x):
            return(val1)
        else :
            return(val2)
    elif x > val2 and x < val3:
        if (x - val2) < (val3 - x):
            return(val2)
        else :
            return(val3)

rounding = spark.udf.register("rounding", func_round)

updated_df = rdd_df.select(rounding(col('ascension')),rounding(col('declination')),rounding(col('frequency')),col('time'))

updated_df.show(n = 15)

# Renaming the columns
updated_df = updated_df.withColumnRenamed('rounding(ascension)', 'ascension')
updated_df = updated_df.withColumnRenamed('rounding(declination)', 'declination')
updated_df = updated_df.withColumnRenamed('rounding(frequency)', 'frequency')

# Counting number of occurrences  in descending order
count = updated_df.groupBy('ascension', 'declination', 'frequency').count()
count.sort("count", ascending = False).show(n = 15)

# Error control (2.5 std)
filtered_count = updated_df.filter(((updated_df.ascension == 104.5) | (updated_df.ascension == 104) | (updated_df.ascension == 105)) & ((updated_df.declination == 82) | (updated_df.declination == 81.5) | (updated_df.declination == 82.5)) & ((updated_df.frequency == 2231.5) | (updated_df.frequency == 2231) | (updated_df.frequency == 2232)))

filtered_count = filtered_count.sort('time', ascending = True)
filtered_count.show(n = filtered_count.count())

# We found time period of approximately 2.2 seconds by roughly observing the data

# Count number of occurrences with error controlled
filtered_count.count() #34 blips

# Finding more accurate time period than 2.2 seconds
time_period = (filtered_count.select(['time']).collect()[-1][0] - filtered_count.select(['time']).collect()[0][0])/filtered_count.count()
print("The time period is", time_period) # approximately 2.135 seconds
