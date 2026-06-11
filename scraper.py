import requests
from bs4 import BeautifulSoup
import csv
import os
from datetime import datetime, timedelta
import time
import re

TRAIN_CODES = ["12419", "12429", "12231", "12540", "14203", "14204", "14210", "14216", "14220", "14228", "14262", "19402", "20402", "22122", "22684","11110", "11408", "12003", "12104", "12179", "12209", "12210", "12229", "12530", "12532", "12533", "12535", "12583", "12593", "15008", "15034", "15043", "15072", "15204", "15205", "22453", "22545", "82501", "55050", "55066", "55345","15032","15011"]

def extract_minutes(text):
    if not text or "On Time" in text: return 0
    hrs = re.search(r'(\d+)\s*hr', text)
    mins = re.search(r'(\d+)\s*min', text)
    return (int(hrs.group(1)) * 60 if hrs else 0) + (int(mins.group(1)) if mins else 0)

def extract_date_from_text(row_text, journey_year):
    match = re.search(r'(\d{1,2})[\s-]([a-zA-Z]{3})(?:[\s-](\d{4}))?', row_text)
    if match:
        day = match.group(1)
        month_str = match.group(2)
        year = match.group(3)
        if not year:
            year = journey_year
            if month_str.lower() == 'jan' and datetime.now().month == 12: 
                 year = int(year) + 1
        date_str = f"{day}-{month_str}-{year}"
        try:
            dt_obj = datetime.strptime(date_str, "%d-%b-%Y")
            return dt_obj.strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None

def run_scraper():
    # Target: 2 days ago (Ensures stable tracking data availability on ConfirmTkt)
    target_date = datetime.now() - timedelta(days=2)
    
    url_date = target_date.strftime("%d-%b-%Y") 
    db_origin_date = target_date.strftime("%Y-%m-%d") 
    day_name = target_date.strftime("%A")
    journey_year = target_date.year

    csv_file = 'train_delays.csv'
    file_exists = os.path.isfile(csv_file)
    
    try:
        f = open(csv_file, 'a', newline='', encoding='utf-8')
        writer = csv.writer(f)
        if not file_exists:
            # Matches your original MySQL database columns exactly
            writer.writerow(['train_no', 'station_name', 'station_index', 'arrival_time', 'departure_time', 'delay_minutes', 'journey_date', 'actual_date', 'is_mid_route', 'day_of_week'])
    except Exception as e:
        print(f"File Error: {e}")
        return

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}

    print(f"--- Starting Scrape for Origin Date: {day_name}, {url_date} ---")

    for train in TRAIN_CODES:
        url = f"https://www.confirmtkt.com/train-running-status/{train}?Date={url_date}"
        try:
            res = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            status_text = soup.find('div', class_='train-update__status')
            status_msg = status_text.get_text().lower() if status_text else ""
            
            if "yet to start" in status_msg or "not started" in status_msg:
                continue

            is_mid = 1 if "reached" not in status_msg and "departed" in status_msg else 0
            
            rows = soup.find_all('div', class_='rs__station-row')
            total_stations = len(rows)

            for index, row in enumerate(rows, start=1):
                name_elem = row.find('span', class_='rs__station-name')
                delay_div = row.find('div', class_='rs__station-delay')
                cols = row.find_all('div', class_='col-xs-2')

                if name_elem and len(cols) >= 2:
                    stn = name_elem.get_text(strip=True)
                    arrival = cols[0].get_text(strip=True)
                    departure = cols[1].get_text(strip=True)

                    if index == 1: arrival = "ORIGIN"
                    if index == total_stations: departure = "TERMINATED"

                    mins = extract_minutes(delay_div.get_text() if delay_div else "")
                    row_text = row.get_text(" ", strip=True) 
                    actual_date_val = extract_date_from_text(row_text, journey_year)
                    
                    if not actual_date_val:
                        actual_date_val = db_origin_date

                    writer.writerow([train, stn, index, arrival, departure, mins, db_origin_date, actual_date_val, is_mid, day_name])
            
            print(f"Logged Train {train} for {db_origin_date}")
            time.sleep(1.5)
            
        except Exception as e:
            print(f"Error processing Train {train}: {e}")

    f.close()
    print("Cycle Complete.")

if __name__ == "__main__":
    run_scraper()
