
# coding: utf-8

# In[1]:


from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg, when, mean
from pyspark.sql import functions as F
from pyspark.sql import DataFrameStatFunctions as statFunc
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import pandas as pd
import os
import pyarrow.parquet as pq
import pyarrow as pa


# In[29]:


from pyspark.sql.functions import concat, col, lit


# In[2]:


appName="Test"
conf = SparkConf().setAppName(appName)
sc =   SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark=SparkSession.builder.appName(appName).config("spark.some.config.option", "some-value").getOrCreate()


# In[6]:


#df = spark.read.csv('C:\\Users\\sharmistha.ghosh\\Downloads\\data.csv',header=True)  # assuming the file contains a header


# In[3]:


df = pd.read_csv('C:\\Users\\sharmistha.ghosh\\Downloads\\data.csv')


# In[4]:


df["Values"]= df["Values"].str.split(expand = True)


# In[5]:


df[['Values1','Values2','Values3','Values4','Values5']] = df.Values.str.split(";",expand=True,)


# In[6]:


df.head(10)


# In[7]:


df.drop(['Values'], axis=1, inplace = True)


# In[8]:


table = pa.Table.from_pandas(df)


# In[9]:


pq.write_table(table, 'C:\\Users\\sharmistha.ghosh\\Downloads\\example.parquet')


# In[10]:


parquetFile = spark.read.parquet("C:\\Users\\sharmistha.ghosh\\Downloads\\example.parquet")


# In[11]:


parquetFile.createOrReplaceTempView("parquetFile")
df1 = spark.sql("SELECT * FROM parquetFile")
df1.show()


# In[12]:


import pyspark.sql.functions as fn


# In[13]:


df2 = df1.cache().groupBy("Country").agg(fn.sum("Values1"))
df3 = df1.cache().groupBy("Country").agg(fn.sum("Values2"))
df7 = df1.cache().groupBy("Country").agg(fn.sum("Values3"))
df5 = df1.cache().groupBy("Country").agg(fn.sum("Values4"))
df6 = df1.cache().groupBy("Country").agg(fn.sum("Values5"))


# In[14]:


df2.show()


# In[15]:


df_1 = df2.select(col("Country").alias("Country1"), col("sum(Values1)").alias("valu1"))
df_3 = df7.select(col("Country").alias("Country3"), col("sum(Values3)").alias("valu3"))
df_4 = df5.select(col("Country").alias("Country4"), col("sum(Values4)").alias("valu4"))
df_5 = df6.select(col("Country").alias("Country5"), col("sum(Values5)").alias("valu5"))
df_2 = df3.select(col("Country").alias("Country2"), col("sum(Values2)").alias("valu2"))


# In[248]:


df_5.show()


# In[16]:


df1T = df_1.alias('df1T')
df2T = df_2.alias('df2T')
df3T = df_3.alias('df3T')
df4T = df_4.alias('df4T')
df5T = df_5.alias('df5T')


# In[244]:


df1T.show()


# In[17]:


inner_join1 = df1T.join(df2T, df1T.Country1 == df2T.Country2)
inner_join2 = df3T.join(df4T, df3T.Country3 == df4T.Country4)
inner_join3 = df1T.join(df5T, df1T.Country1 == df5T.Country5)


# In[18]:


inner_join1 = inner_join1.drop(inner_join1.Country2)
inner_join2 = inner_join2.drop(inner_join2.Country4)
inner_join3 = inner_join3.drop(inner_join3.Country1)


# In[19]:



inner_join1.show()
inner_join2.show()
inner_join3.show()


# In[20]:


inner_join23 = inner_join1.join(inner_join2, df1T.Country1 == inner_join2.Country3)
inner_join34 = inner_join23.join(inner_join3, inner_join23.Country1 == inner_join3.Country5)
#inner_join3 = df1T.join(df5T, df1T.Country1 == df5T.Country5)


# In[21]:


inner_join34 = inner_join34.drop(inner_join34.Country3)
inner_join34 = inner_join34.drop(inner_join34.Country5)


# In[22]:


inner_join34.show()

