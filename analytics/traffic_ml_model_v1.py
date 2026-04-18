import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import joblib

df = pd.read_csv('data/clean/traffic_smartcity_clean_v1.csv')

df['datetime'] = pd.to_datetime(df['datetime'])
df['hour'] = df['datetime'].dt.hour
df['day'] = df['datetime'].dt.dayofweek

df['lag1'] = df['traffic'].shift(1)
df = df.dropna()

X = df[['hour', 'day', 'lag1']]
y = df['traffic']

model = RandomForestRegressor()
model.fit(X, y)

joblib.dump(model, 'models/traffic_model_v1.pkl')

print("Model berhasil disimpan")