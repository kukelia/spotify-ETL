import spotipy
import pandas as pd
from datetime import datetime

TOP_50_PLAYLIST_ID = '37i9dQZEVXbMMy2roB9myp'

spotipy_object = spotipy.Spotify(client_credentials_manager=spotipy.oauth2.SpotifyClientCredentials(client_id="2d33cb98e7304ac2958a0d6e5a765a56", client_secret='da989bfc36ff49139522ef5a58867cb5'))

def extract(id: str):
    return spotipy_object.playlist(playlist_id=id)


def transform(raw_data) -> pd.DataFrame:
    myList = []
    pos = 1 #CHECK THIS
    for song in raw_data['tracks']['items']:
        myList.append(
            {
                'name' : song['track']['name'],
                'artist' : song['track']['artists'][0]['name'], #only care for main artist
                # 'position' : song['track']['name'],
                'rank' : pos,
                'date' : datetime.now().date()
            }
        )
        pos +=1
    df = pd.DataFrame(myList)
    
    if df.empty:
        raise Exception("error: dataframe is empty")
        
    if df['rank'].dtype != 'int64':
        raise Exception("error: ranking is not numeric type")

    if df.shape[0] !=50:
        raise Exception("error: dataframe size is not 50")
    
    if df.isnull().values.any() == 1:
        raise Exception("error: null values")
        
    df['date'] = pd.to_datetime(df['date'])
    return df


def load(df: pd.DataFrame):
    from engine import engine
    df.to_sql('top_50_arg_songs', index=False, if_exists='append')


if __name__ == "__name__":
    
    raw_data = extract(TOP_50_PLAYLIST_ID)

    df = transform(raw_data)

    load(df)

    print('ETL successful')