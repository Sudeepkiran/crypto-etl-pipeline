import requests
import pandas as pd
import sqlite3
import datetime

# Step 1: Extract
url = "https://api.coingecko.com/api/v3/coins/markets"
params = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 10,
    "page": 1,
    "sparkline": "false"
}
response = requests.get(url, params=params)
data = response.json()

# Step 2: Transform
df = pd.DataFrame(data)
df = df[["id", "symbol", "name", "current_price", "market_cap", "total_volume"]]
df["timestamp"] = datetime.datetime.now()

# Save to CSV
df.to_csv("data/crypto_data.csv", index=False)

# Step 3: Load into SQLite
conn = sqlite3.connect("data/crypto.db")
df.to_sql("crypto_prices", conn, if_exists="append", index=False)
conn.close()

print("ETL complete.")

