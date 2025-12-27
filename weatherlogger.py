import requests
import pandas as pd
from datetime import datetime
import pytz
import os
import math

def calculate_dew_point(temp, humidity):
    a = 17.27
    b = 237.7
    alpha = ((a * temp) / (b + temp)) + math.log(humidity / 100.0)
    return (b * alpha) / (a - alpha)

def run_batched_scraper():
    ist = pytz.timezone('Asia/Kolkata')
    now_ist = datetime.now(ist)
    formatted_hour = now_ist.strftime('%Y-%m-%d %H:00:00')
    hour_idx = now_ist.hour 

    if not os.path.exists("station_coords.csv"):
        return
        
    df_coords = pd.read_csv("station_coords.csv")
    results = []
    batch_size = 50 
    
    for i in range(0, len(df_coords), batch_size):
        batch = df_coords.iloc[i : i + batch_size]
        lats = ",".join(batch['latitude'].astype(str))
        lons = ",".join(batch['longitude'].astype(str))
        
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lats}&longitude={lons}&hourly=relative_humidity_2m,temperature_2m&timezone=Asia/Kolkata&forecast_days=1"
        
        try:
            res = requests.get(url).json()
            if isinstance(res, dict): res = [res] 

            for idx, data in enumerate(res):
                temp = data['hourly']['temperature_2m'][hour_idx]
                hum = data['hourly']['relative_humidity_2m'][hour_idx]
                
                dp = calculate_dew_point(temp, hum)
                spread = temp - dp
                
                if spread < 1.0 and hum > 95:
                    risk = 3
                elif spread < 2.0 and hum > 90:
                    risk = 2
                elif spread < 3.0:
                    risk = 1
                else:
                    risk = 0

                results.append({
                    "station_name": batch.iloc[idx]['station_name'],
                    "weather_timestamp": formatted_hour,
                    "humidity": hum,
                    "temp_c": temp,
                    "dew_point": round(dp, 2),
                    "temp_spread": round(spread, 2),
                    "fog_risk": risk
                })
        except:
            continue

    if results:
        out_df = pd.DataFrame(results)
        file_path = "weather_log.csv"
        hdr = not os.path.exists(file_path)
        out_df.to_csv(file_path, mode='a', index=False, header=hdr)

if __name__ == "__main__":
    run_batched_scraper()