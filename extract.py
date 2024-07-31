import requests
from google.cloud import storage, bigquery
import datetime as dt
import csv
import os

api_key = '490ecf09fdbb8860bc092ab070555c99'
base_url = 'http://api.openweathermap.org/data/2.5/weather?q={}&appid={}'

cities = [
    'Mumbai', 'Delhi', 'Bangalore', 'Hyderabad', 'Ahmedabad', 'Chennai', 'Kolkata', 
     'Surat', 'Pune', 'Jaipur', 'Lucknow', 'Kanpur', 'Nagpur', 'Indore', 'Thane', 
    'Bhopal', 'Visakhapatnam', 'Pimpri-Chinchwad', 'Patna', 'Vadodara', 'Ghaziabad', 
    'Ludhiana', 'Agra', 'Nashik', 'Faridabad', 'Meerut', 'Rajkot', 'Kalyan-Dombivli', 
     'Vasai-Virar', 'Varanasi', 'Srinagar', 'Aurangabad', 'Dhanbad', 'Amritsar', 
     'Navi Mumbai', 'Allahabad', 'Ranchi', 'Howrah', 'Coimbatore', 'Jabalpur', 
    'Gwalior', 'Vijayawada', 'Jodhpur', 'Madurai', 'Raipur', 'Kota', 'Guwahati', 
    'Chandigarh', 'Solapur', 'Hubballi-Dharwad', 'Mysore', 'Tiruchirappalli', 
     'Bareilly', 'Aligarh', 'Tiruppur', 'Moradabad', 'Jalandhar', 'Bhubaneswar', 
     'Salem', 'Warangal', 'Guntur', 'Bhiwandi', 'Saharanpur', 'Gorakhpur', 'Bikaner', 
     'Amravati', 'Noida', 'Jamshedpur', 'Bhilai', 'Cuttack', 'Firozabad', 'Kochi', 
    'Nellore', 'Bhavnagar', 'Dehradun', 'Durgapur', 'Asansol', 'Rourkela', 'Nanded', 
    'Kolhapur', 'Ajmer', 'Akola', 'Gulbarga', 'Jamnagar', 'Ujjain', 'Loni', 'Siliguri', 
     'Jhansi', 'Ulhasnagar', 'Jammu', 'Sangli-Miraj & Kupwad', 'Mangalore', 'Erode', 
    'Belgaum', 'Kurnool', 'Ambattur', 'Tirunelveli', 'Malegaon'
]



data = []
for city in cities:
    url = base_url.format(city, api_key)
    response = requests.get(url)
    if response.status_code == 200:
        
        data.append(response.json())
        

    

def kelvin_to_celsius_fahrenheit(kelvin):
    celsius = kelvin - 273.15
    fahrenheit = celsius * (9 / 5) + 32
    return celsius, fahrenheit

for i in range(len(data)):
    temp_kelvin = data[i]['main']['temp']
    temp_celsius, temp_fahrenheit = kelvin_to_celsius_fahrenheit(temp_kelvin)
    feels_like_kelvin = data[i]['main']['feels_like']
    feels_like_celsius, feels_like_fahrenheit = kelvin_to_celsius_fahrenheit(feels_like_kelvin)
    sunrise_time = dt.datetime.utcfromtimestamp(data[i]['sys']['sunrise'] + data[i]['timezone'])
    
    data[i]['main']['temp_celsius'] = temp_celsius
    data[i]['main']['temp_fahrenheit'] = temp_fahrenheit
    data[i]['main']['feels_like_celsius'] = feels_like_celsius
    data[i]['main']['feels_like_fahrenheit'] = feels_like_fahrenheit
    data[i]['sys']['sunrise_time'] = sunrise_time.isoformat()
 




client = storage.Client(project='etl-pipeline-430513')
bucket = client.get_bucket('etl-weatherdata')






# Convert JSON to CSV
csv_file = 'weather_data.csv'
csv_columns = [
    "coord_lon", "coord_lat", "weather_id", "weather_main", "weather_description", "weather_icon",
    "base", "main_temp", "main_feels_like", "main_temp_min", "main_temp_max", "main_pressure",
    "main_humidity", "main_sea_level", "main_grnd_level", "main_temp_celsius", "main_temp_fahrenheit",
    "main_feels_like_celsius", "main_feels_like_fahrenheit", "visibility", "wind_speed", "wind_deg",
    "clouds_all", "dt", "sys_type", "sys_id", "sys_country", "sys_sunrise", "sys_sunset", "sys_sunrise_time",
    "timezone", "id", "name", "cod"
]

with open(csv_file, 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
    writer.writeheader()
    for item in data:
        csv_row = {
            "coord_lon": item["coord"]["lon"],
            "coord_lat": item["coord"]["lat"],
            "weather_id": item["weather"][0]["id"],
            "weather_main": item["weather"][0]["main"],
            "weather_description": item["weather"][0]["description"],
            "weather_icon": item["weather"][0]["icon"],
            "base": item["base"],
            "main_temp": item["main"]["temp"],
            "main_feels_like": item["main"]["feels_like"],
            "main_temp_min": item["main"]["temp_min"],
            "main_temp_max": item["main"]["temp_max"],
            "main_pressure": item["main"]["pressure"],
            "main_humidity": item["main"]["humidity"],
            "main_sea_level": item["main"]["sea_level"],
            "main_grnd_level": item["main"]["grnd_level"],
            "main_temp_celsius": item["main"]["temp_celsius"],
            "main_temp_fahrenheit": item["main"]["temp_fahrenheit"],
            "main_feels_like_celsius": item["main"]["feels_like_celsius"],
            "main_feels_like_fahrenheit": item["main"]["feels_like_fahrenheit"],
            "visibility": item["visibility"],
            "wind_speed": item["wind"]["speed"],
            "wind_deg": item["wind"]["deg"],
            "clouds_all": item["clouds"]["all"],
            "dt": item["dt"],
            "sys_type": item["sys"].get("type", None),
            "sys_id": item["sys"].get("id",None),
            "sys_country": item["sys"]["country"],
            "sys_sunrise": item["sys"]["sunrise"],
            "sys_sunset": item["sys"]["sunset"],
            "sys_sunrise_time": item["sys"]["sunrise_time"],
            "timezone": item["timezone"],
            "id": item["id"],
            "name": item["name"],
            "cod": item["cod"]
        }
        writer.writerow(csv_row)

print(f"Data successfully written to {csv_file}")

os.environ["GOOGLE_CLOUD_PROJECT"] = "etl-pipeline-430513"

blob = bucket.blob('weather_data.csv')
blob.upload_from_filename('weather_data.csv')



def load_csv_to_bigquery(dataset_id, table_id, source_uri):
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )
    load_job = client.load_table_from_uri(source_uri, table_ref, job_config=job_config)
    load_job.result()
    print(f"Loaded {load_job.output_rows} rows into {dataset_id}:{table_id}.")
    
project_id = 'etl-pipeline-430513'
dataset_id = 'etl_dataset'
table_id = 'etl-table-csv'
source_uri = f'gs://etl-weatherdata/weather_data.csv'
    
load_csv_to_bigquery(dataset_id, table_id, source_uri)


