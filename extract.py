import requests
import json
import os
from google.cloud import storage
api_key = 'mykey'
base_url = 'http://api.openweathermap.org/data/2.5/weather?q={}&appid={}'

cities = ['New York', 'London', 'Paris', 'Tokyo']

data = []
for city in cities:
    url = base_url.format(city, api_key)
    response = requests.get(url)
    if response.status_code == 200:
        data.append(response.json())
print(data)
# Save data to a JSON file
with open('weather_data.json', 'w') as f:
    json.dump(data, f)
# Initialize a GCS client
client = storage.Client()
bucket = client.get_bucket('bkt-weatherdata')

# Upload JSON file to GCS
blob = bucket.blob('weather_data.json')
blob.upload_from_filename('weather_data.json')
