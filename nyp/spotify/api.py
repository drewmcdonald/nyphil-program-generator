import json
from base64 import b64encode
from requests import post, get
from .credentials import SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET

auth_url = 'https://accounts.spotify.com/api/token'
auth_header = {"Authorization": "Basic {}".format(b64encode(str.encode(SPOTIFY_CLIENT_ID + ":" + SPOTIFY_CLIENT_SECRET)).decode())}
auth = post(auth_url, headers=auth_header, data={'grant_type': 'client_credentials'})
auth = json.loads(auth.content)

search_header = {'Authorization': "Bearer {}".format(auth['access_token'])}
search_url = 'https://api.spotify.com/v1/search'
search_params = {'q': 'mussorgsky',
                 'type': 'artist'}
search = get(search_url, params=search_params, headers=search_header)
