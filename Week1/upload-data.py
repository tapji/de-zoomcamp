#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


pd.__version__


# In[3]:


from sqlalchemy import create_engine


# In[4]:


get_ipython().system("pip install psycopg2-binary")


# In[5]:


engine = create_engine("postgresql://root:root@localhost:5432/ny_taxi")


# In[6]:


engine.connect()


# ### Question 3:

# In[7]:


df1 = pd.read_csv("green_tripdata_2019-09.csv.gz")


# In[8]:


print(pd.io.sql.get_schema(df1, name="green_taxi_data", con=engine))


# In[9]:


df1.head(n=0).to_sql(name="green_taxi_data", con=engine, if_exists="replace")


# In[10]:


df1_iter = pd.read_csv("green_tripdata_2019-09.csv.gz", iterator=True, chunksize=100000)


# In[11]:


df1


# In[12]:


from time import time


# In[13]:


while True:
    try:
        t_start = time()

        df1 = next(df1_iter)

        df1.lpep_pickup_datetime = pd.to_datetime(df1.lpep_pickup_datetime)
        df1.lpep_dropoff_datetime = pd.to_datetime(df1.lpep_dropoff_datetime)

        df1.to_sql(name="green_taxi_data", con=engine, if_exists="append")

        t_end = time()
        print("inserted another chunk, took %.3f second" % (t_end - t_start))

    except StopIteration:
        print("End of file reached.")
        break


# In[14]:


df1.info()


# In[15]:


get_ipython().system("wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv")


# In[16]:


df_zones = pd.read_csv("taxi+_zone_lookup.csv")


# In[17]:


df_zones.head()


# In[20]:


df_zones.to_sql(name="zones", con=engine, if_exists="replace")


# In[ ]:
