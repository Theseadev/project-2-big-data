import pandas as pd

df = pd.read_csv('data/raw/traffic_smartcity_v1.csv')

df['datetime'] = pd.to_datetime(df['datetime'])
df = df.sort_values('datetime')
df = df.dropna()

df.to_csv('data/clean/traffic_smartcity_clean_v1.csv', index=False)

print("Data cleaning selesai")