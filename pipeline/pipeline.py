# import sys
# import pandas as pd


# print ('Hello pipeline')
# print ('Arguments', sys.argv)
# month = int(sys.argv[1])
# print (f'Current Month ={month}')

# df = pd.DataFrame({"Day ": [1, 2], "Num of passangers": [3, 4]})
# df['month'] = month
# print(df.head())

# df.to_parquet(f"output_{month}.parquet_")

# df.to_parquet(f"output_day_{sys.argv[1]}.parquet")


