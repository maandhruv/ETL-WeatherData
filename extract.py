import requests
import json
from google.cloud import storage
import datetime as dt

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
    print(data[i])


with open('weather_data.json', 'w') as f:
    json.dump(data, f, indent=4)


client = storage.Client(project='etl-pipeline-430513')
bucket = client.get_bucket('etl-weatherdata')


blob = bucket.blob('weather_data.json')
blob.upload_from_filename('weather_data.json')
