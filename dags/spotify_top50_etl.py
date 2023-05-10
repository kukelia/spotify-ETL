import spotipy
import pandas as pd
from datetime import datetime
from sqlalchemy import text
from engine import engine

def extract(id: str, spotipy_object):
    return spotipy_object.playlist(playlist_id=id)


def transform(raw_data) -> pd.DataFrame:
    global today
    myList = []
    pos = 1 #CHECK THIS
    for song in raw_data['tracks']['items']:
        myList.append(
            {
                'song_name' : song['track']['name'],
                'artist' : song['track']['artists'][0]['name'], #only care for main artist
                # 'position' : song['track']['name'],
                'rank' : pos,
                'extract_date' : today
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
        
    df['extract_date'] = pd.to_datetime(df['extract_date'])
    return df


def load(df: pd.DataFrame):
    df.to_sql('top_50_arg_songs',con=engine, index=False, if_exists='append')


def run_top50_etl():
    print("empieza proceso ETL")
    top_50_playlist_id = '37i9dQZEVXbMMy2roB9myp'
    spotipy_object = spotipy.Spotify(client_credentials_manager=spotipy.oauth2.SpotifyClientCredentials(client_id="2d33cb98e7304ac2958a0d6e5a765a56", client_secret='da989bfc36ff49139522ef5a58867cb5'))

    today = datetime.now().date()
    conn = engine.connect()
    query = text(f"SELECT * FROM top_50_arg_songs WHERE extract_date = '{today}' LIMIT 1;")

    #check if script has already ran today (avoid duplicates)
    if conn.execute(query).fetchone() != None:
        raise Exception("there are already records extracted today")

    raw_data = extract(top_50_playlist_id, spotipy_object)
    print("data extracted")

    df = transform(raw_data)
    print("data transformed")

    load(df)
    print("data loaded")

    print('ETL successful')