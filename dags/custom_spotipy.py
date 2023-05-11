from spotipy import Spotify
from spotipy import SpotifyOAuth

class Spoti2(Spotify):
    
    spoty_obj = None
    
    def __new__(cls,*args,**kwargs):
        if cls.spoty_obj is None:
            cls.spoty_obj = super().__new__(cls)
            print('se creo instancia')
        return cls.spoty_obj
    def __init__(self, **args):
        super().__init__(**args)


scope='user-read-recently-played'
print('actualizado')
creds = SpotifyOAuth(scope=scope, client_id='2d33cb98e7304ac2958a0d6e5a765a56', client_secret='da989bfc36ff49139522ef5a58867cb5', redirect_uri='http://127.0.0.1:9090',cache_path='./dags/.cache')
sp = Spoti2(auth_manager=creds)