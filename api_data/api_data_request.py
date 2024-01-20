import requests

base_url = "https://v3.football.api-sports.io/"

with open('settings/api_key.txt', 'r') as api_key_file:
    api_key = api_key_file.read()

headers = {
    'x-rapidapi-key': api_key,
    'x-rapidapi-host': 'v3.football.api-sports.io'
}

def make_request(base_url, endpoint, parameters, headers):
    response = requests.get(f'{base_url}{endpoint}{parameters}', headers=headers)
    return response.json().get('response', {})