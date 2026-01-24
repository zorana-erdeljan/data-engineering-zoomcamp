#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm



# prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
# url = f'{prefix}/yellow_tripdata_2021-01.csv.gz'
# df = pd.read_csv(url)
# df.head()

# engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
# In[2]:


# len(df)






# df['VendorID']



# df['tpep_pickup_datetime']


# In[10]:


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

# df = pd.read_csv(
#     url,
#     dtype=dtype,
#     parse_dates=parse_dates
# )


# In[7]:


# df.head()


# # In[8]:


# df


# In[9]:


# df['tpep_pickup_datetime']


# # In[10]:


# get_ipython().system('uv add sqlalchemy')


# # In[11]:


# get_ipython().system('uv add psycopg2-binary')


# In[18]:
def run():

    pg_name= 'root'
    pg_pass= 'root'
    pg_host = 'localhost'
    pg_port= 5432
    pg_db= 'ny_taxi'

    year = 2021
    month = 1

    targer_table = 'yellow_taxi_data'

    chunck_size = 100000

    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = f'{prefix}/yellow_tripdata_2021-01.csv.gz'
    engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunck_size
    )

    first = True

    for df_chunk in tqdm(df_iter):

        if first:
            # Create table schema (no data)
            df_chunk.head(0).to_sql(
                name=targer_table,
                con=engine,
                if_exists="replace"
            )
            first = False
            print("Table created")

        # Insert chunk
        df_chunk.to_sql(
            name=targer_table,
            con=engine,
            if_exists="append"
        )

        print("Inserted:", len(df_chunk))



if __name__ == '__main__':
    run()


# print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))

# df.head(0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

# get_ipython().system('uv add tqdm')

# for df_chunk in tqdm(df_iter):
#     df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')


