#!/usr/bin/env python
# coding: utf-8

# In[164]:


import pyspark;
from pyspark.sql import SparkSession;
from pyspark.sql.types import StructType
from pyspark.sql.functions import split
from pyspark.sql.functions import col,desc, concat, lit
from pyspark.sql import *


# In[19]:


spark = SparkSession.builder.master("local").appName("Movie").getOrCreate()


# In[20]:


booksSchema = StructType() \
                    	.add("user_id", "integer")\
                    	.add("movie_id", "integer")\
                    	.add("rating", "integer")\
                    	.add("timestamp", "string")


# In[21]:


booksdata = spark.read.text("hdfs://localhost:9000/Movie/Input/ratings.dat")


# In[22]:


# booksdata.show();


# In[23]:


booksdata_1 = booksdata.withColumn('user_id', split(booksdata['value'], "::").getItem(0))\
                        .withColumn('movie_id', split(booksdata['value'], "::").getItem(1))\
                        .withColumn('rating', split(booksdata['value'], "::").getItem(2))\
                        .withColumn('timestamp', split(booksdata['value'], "::").getItem(3))


# In[24]:


booksdata_1 = booksdata_1.drop("value")


# In[46]:


# booksdata_1.show(truncate=False)


# In[169]:


booksdata_2 = booksdata_1.groupBy("movie_id").count().sort(col("movie_id"));


# In[172]:


rating_count = booksdata_2.select(concat(col("movie_id").cast("string"),lit(' '),col("count").cast("string")).alias("value"))


# In[173]:


# rating_count.show()


# In[174]:


rating_count.write.save("hdfs://localhost:9000/Movie/output2/", format = 'text', mode = 'append')


# In[175]:


booksdata_3 = booksdata_1.groupBy("movie_id" ).count().sort(col("count" ).desc()).limit(25);


# In[176]:


top_k_rate = booksdata_3.select(concat(col("movie_id").cast("string"),lit(' '),col("count").cast("string")).alias("value"))


# In[180]:


# top_k_rate.show();


# In[181]:


top_k_rate.write.save("hdfs://localhost:9000/Movie/output3/", format = 'text', mode = 'append')


# In[ ]:





# In[ ]:




