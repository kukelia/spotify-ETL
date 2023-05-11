from spotipy import SpotifyOAuth
from spotipy import Spotify
scope='user-read-recently-played'

creds = SpotifyOAuth(scope=scope, client_id='2d33cb98e7304ac2958a0d6e5a765a56', client_secret='da989bfc36ff49139522ef5a58867cb5', redirect_uri='http://127.0.0.1:9090')
sp = Spotify(auth_manager=creds)
if __name__ =='__main__':
    
    print(sp)
    from time import sleep
    sleep(3)
