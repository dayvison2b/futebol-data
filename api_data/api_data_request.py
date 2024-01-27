import requests

base_url = "https://v3.football.api-sports.io/"

def read_api_key():
    with open('settings/api_key.txt', 'r') as api_key_file:
        return api_key_file.read().strip()

headers = {
    'x-rapidapi-key': read_api_key(),
    'x-rapidapi-host': 'v3.football.api-sports.io'
}

async def make_request_async(session, base_url, endpoint, parameters, headers):
    url = f"{base_url}/{endpoint}{parameters}"

    response = await session.get(url, headers=headers)
    response.raise_for_status()

    return response.json().get('response', {})

def make_request(base_url, endpoint, parameters, headers):
    response = requests.get(f'{base_url}{endpoint}{parameters}', headers=headers)
    return response.json().get('response', {})