from pyspark.sql.functions import *
from pyspark.sql.window import Window
'''streaming_data=[("U1","2019-01-01T11:00:00Z") ,
        ("U1","2019-01-01T11:15:00Z") ,
        ("U1","2019-01-01T12:00:00Z") ,
        ("U1","2019-01-01T12:20:00Z") ,
        ("U1","2019-01-01T15:00:00Z") ,
        ("U2","2019-01-01T11:00:00Z") ,
        ("U2","2019-01-02T11:00:00Z") ,
        ("U2","2019-01-02T11:25:00Z") ,
        ("U2","2019-01-02T11:50:00Z") ,
        ("U2","2019-01-02T12:15:00Z") ,
        ("U2","2019-01-02T12:40:00Z") ,
        ("U2","2019-01-02T13:05:00Z") ,
        ("U2","2019-01-02T13:20:00Z") ]'''

streaming_data=[("U1","2019-01-01T11:00:00Z") ,
        ("U1","2019-01-01T11:15:00Z") ,
        ("U1","2019-01-01T12:00:00Z") ,
        ("U1","2019-01-01T12:20:00Z") ,
        ("U1","2019-01-01T15:00:00Z") ,
        ("U2","2019-01-01T11:00:00Z") ,
        ("U2","2019-01-02T11:00:00Z") ,
        ("U2","2019-01-02T11:25:00Z") ,
        ("U2","2019-01-02T11:50:00Z") ,
        ("U2","2019-01-02T12:15:00Z") ,
        ("U2","2019-01-02T12:40:00Z") ,
        ("U2","2019-01-02T13:05:00Z") ,
        ("U2","2019-01-02T13:20:00Z") ]

schema=("UserId","Click_Time")
# Partition the dataset based on user_id and order it using click time
window_spec=Window.partitionBy("UserId").orderBy("Click_Time")
# Create the data frame from the given input
df_stream=spark.createDataFrame(streaming_data,schema)
# Convert the given time into timestamp
df_stream=df_stream.withColumn("Click_Time",df_stream["Click_Time"].cast("timestamp"))
# now get the time difference for every click, lag give us the value of the offset row
df_stream=df_stream.withColumn("time_diff", (unix_timestamp("Click_Time")- unix_timestamp(lag(col("Click_Time"),1).over(window_spec)))/60).na.fill(0)
# Now check if any difference is greater than 30 minutes if yes then 1 else 0
df_stream=df_stream.withColumn("cond_", when(col("time_diff")>30,1).otherwise(0))
# create session based on 30 minutes, here we calculate based on above value,
# whenever we found 1 it means session has changed, that time we need to increment the session as below
df_stream=df_stream.withColumn("temp_session",sum(col("cond_")).over(window_spec))
'''
+------+-------------------+---------+-----+------------+-------------+----------------+--------------------+
|UserId|Click_Time         |time_diff|cond_|temp_session|2hr_time_diff|temp_session_2hr|final_session_groups|
+------+-------------------+---------+-----+------------+-------------+----------------+--------------------+
|U2    |2019-01-01 11:00:00|0.0      |0    |0           |0            |0               |0                   |
|U2    |2019-01-02 11:00:00|1440.0   |1    |1           |0            |0               |0                   |
|U2    |2019-01-02 11:25:00|25.0     |0    |1           |1500         |0               |0                   |
|U2    |2019-01-02 11:50:00|25.0     |0    |1           |1500         |0               |0                   |
|U2    |2019-01-02 12:15:00|25.0     |0    |1           |1500         |0               |0                   |
|U2    |2019-01-02 12:40:00|25.0     |0    |1           |1500         |0               |0                   |
|U2    |2019-01-02 13:05:00|25.0     |0    |1           |1500         |1               |0                   |
|U2    |2019-01-02 13:20:00|15.0     |0    |1           |900          |1               |0                   |
|U1    |2019-01-01 11:00:00|0.0      |0    |0           |0            |0               |0                   |
|U1    |2019-01-01 11:15:00|15.0     |0    |0           |900          |0               |0                   |
|U1    |2019-01-01 12:00:00|45.0     |1    |1           |0            |0               |0                   |
|U1    |2019-01-01 12:20:00|20.0     |0    |1           |1200         |0               |0                   |
|U1    |2019-01-01 15:00:00|160.0    |1    |2           |0            |0               |0                   |
+------+-------------------+---------+-----+------------+-------------+----------------+--------------------+

'''
new_window=Window.partitionBy("UserId","temp_session").orderBy("Click_Time")
'''
Window.unboundedPreceding -> it means all the rows before current row should be considered.
Window.unboundedFollowing -> it means all the rows after current row should be considered.
Window.currentRow -> it means range start or end at current row.
if I write rowsBetween(1,1), It means, I am considering only 1 previous value, if there is no preceding value then will 
return null.
'''
new_spec=new_window.rowsBetween(Window.unboundedPreceding,Window.currentRow)
cond_2hr=(unix_timestamp("Click_Time")-unix_timestamp(lag(col("Click_Time"),1).over(new_window)))
df_stream=df_stream.withColumn("2hr_time_diff", cond_2hr).na.fill(0)
df_stream=df_stream.withColumn("temp_session_2hr",when(sum(col("2hr_time_diff")).over(new_spec)-(2*60*60)>0,1).otherwise(0))
#new_window_2hr=Window.partitionBy(["UserId","temp_session","temp_session_2hr"]).orderBy("Click_Time")
#hrs_cond_=(when(unix_timestamp(col("Click_Time"))-unix_timestamp(first(col("Click_Time")).over(new_window_2hr))-(2*60*60)>0,1).otherwise(0))
#df_stream=df_stream.withColumn("final_session_groups",hrs_cond_)
df_stream.show(truncate=False)
'''
+------+-------------------+---------+-----+------------+-------------+----------------+--------------------+-------------+
|UserId|Click_Time         |time_diff|cond_|temp_session|2hr_time_diff|temp_session_2hr|final_session_groups|final_session|
+------+-------------------+---------+-----+------------+-------------+----------------+--------------------+-------------+
|U2    |2019-01-01 11:00:00|0.0      |0    |0           |0            |0               |0                   |1            |
|U2    |2019-01-02 11:00:00|1440.0   |1    |1           |0            |0               |0                   |2            |
|U2    |2019-01-02 11:25:00|25.0     |0    |1           |1500         |0               |0                   |2            |
|U2    |2019-01-02 11:50:00|25.0     |0    |1           |1500         |0               |0                   |2            |
|U2    |2019-01-02 12:15:00|25.0     |0    |1           |1500         |0               |0                   |2            |
|U2    |2019-01-02 12:40:00|25.0     |0    |1           |1500         |0               |0                   |2            |
|U2    |2019-01-02 13:05:00|25.0     |0    |1           |1500         |1               |0                   |3            |
|U2    |2019-01-02 13:20:00|15.0     |0    |1           |900          |1               |0                   |3            |
|U1    |2019-01-01 11:00:00|0.0      |0    |0           |0            |0               |0                   |1            |
|U1    |2019-01-01 11:15:00|15.0     |0    |0           |900          |0               |0                   |1            |
|U1    |2019-01-01 12:00:00|45.0     |1    |1           |0            |0               |0                   |2            |
|U1    |2019-01-01 12:20:00|20.0     |0    |1           |1200         |0               |0                   |2            |
|U1    |2019-01-01 15:00:00|160.0    |1    |2           |0            |0               |0                   |3            |
+------+-------------------+---------+-----+------------+-------------+----------------+--------------------+-------------+
'''
#df_stream=df_stream.withColumn("final_session",df_stream["temp_session_2hr"]+df_stream["temp_session"]+df_stream["final_session_groups"]+1)
df_stream=df_stream.withColumn("final_session",df_stream["temp_session_2hr"]+df_stream["temp_session"]+1)
df_stream.show(truncate=False)
df_stream = df_stream.drop("temp_session","time_diff","temp_session_2hr")
df_stream=df_stream.withColumn("session_id",concat(col("UserId"),lit(" session_val----->"),col("final_session")))
df_stream.show(20,0)
"""
+------+-------------------+-----+-------------+-------------+---------------------+
|UserId|Click_Time         |cond_|2hr_time_diff|final_session|session_id           |
+------+-------------------+-----+-------------+-------------+---------------------+
|U2    |2019-01-01 11:00:00|0    |0            |1            |U2 session_val----->1|
|U2    |2019-01-02 11:00:00|1    |0            |2            |U2 session_val----->2|
|U2    |2019-01-02 11:25:00|0    |1500         |2            |U2 session_val----->2|
|U2    |2019-01-02 11:50:00|0    |1500         |2            |U2 session_val----->2|
|U2    |2019-01-02 12:15:00|0    |1500         |2            |U2 session_val----->2|
|U2    |2019-01-02 12:40:00|0    |1500         |2            |U2 session_val----->2|
|U2    |2019-01-02 13:05:00|0    |1500         |3            |U2 session_val----->3|
|U2    |2019-01-02 13:20:00|0    |900          |3            |U2 session_val----->3|
|U1    |2019-01-01 11:00:00|0    |0            |1            |U1 session_val----->1|
|U1    |2019-01-01 11:15:00|0    |900          |1            |U1 session_val----->1|
|U1    |2019-01-01 12:00:00|1    |0            |2            |U1 session_val----->2|
|U1    |2019-01-01 12:20:00|0    |1200         |2            |U1 session_val----->2|
|U1    |2019-01-01 15:00:00|1    |0            |3            |U1 session_val----->3|
+------+-------------------+-----+-------------+-------------+---------------------+
"""


from pyspark.sql import Window
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
from pyspark.sql import functions as func
sc = SparkContext.getOrCreate()
tup = [(1, "a"), (1, "a"), (2, "a"), (1, "b"), (2, "b"), (3, "b")]
df = sqlContext.createDataFrame(tup, ["id", "category"])


"""
+------+-------------------+---------+-----+------------+-------------+----------------+--------------------+-------------+
|UserId|Click_Time         |time_diff|cond_|temp_session|2hr_time_diff|temp_session_2hr|final_session_groups|final_session|
+------+-------------------+---------+-----+------------+-------------+----------------+--------------------+-------------+
|U2    |2019-01-01 11:00:00|0.0      |0    |0           |0            |0               |0                   |1            |
|U2    |2019-01-02 11:00:00|1440.0   |1    |1           |0            |0               |0                   |2            |
|U2    |2019-01-02 11:25:00|25.0     |0    |1           |1500         |0               |0                   |2            |
|U2    |2019-01-02 11:50:00|25.0     |0    |1           |1500         |0               |0                   |2            |
|U2    |2019-01-02 12:25:00|35.0     |1    |2           |2100         |0               |0                   |3            |
|U2    |2019-01-02 12:40:00|15.0     |0    |2           |900          |0               |0                   |3            |
|U2    |2019-01-02 13:05:00|25.0     |0    |2           |1500         |1               |0                   |4            |
|U2    |2019-01-02 13:20:00|15.0     |0    |2           |900          |1               |0                   |4            |
|U1    |2019-01-01 11:00:00|0.0      |0    |0           |0            |0               |0                   |1            |
|U1    |2019-01-01 11:15:00|15.0     |0    |0           |900          |0               |0                   |1            |
|U1    |2019-01-01 12:00:00|45.0     |1    |1           |0            |0               |0                   |2            |
|U1    |2019-01-01 12:20:00|20.0     |0    |1           |1200         |0               |0                   |2            |
|U1    |2019-01-01 15:00:00|160.0    |1    |2           |0            |0               |0                   |3            |
+------+-------------------+---------+-----+------------+-------------+----------------+--------------------+-------------+
"""

u3 11:00
u3 11:30
u3 12:00
u3 12:30
u3 01:00