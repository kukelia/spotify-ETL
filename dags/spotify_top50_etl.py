import spotipy
import pandas as pd
from datetime import datetime
from sqlalchemy import text
from engine import engine

def extract(id: str, spotipy_object):
    return spotipy_object.playlist(playlist_id=id)


def transform(raw_data) -> pd.DataFrame:
    today = datetime.now().date()
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

    today = datetime.now().date()
    conn = engine.connect()
    query = text(f"SELECT * FROM top_50_arg_songs WHERE extract_date = '{today}' LIMIT 1;")

    #check if script has already ran today (avoid duplicates)
    if conn.execute(query).fetchone() != None:
        raise Exception("there are already records extracted today")
    
    from custom_spotipy import sp
    top_50_playlist_id = '37i9dQZEVXbMMy2roB9myp'
    
    raw_data = extract(top_50_playlist_id, sp)
    print("data extracted")

    df = transform(raw_data)
    print("data transformed")

    load(df)
    print("data loaded")

    print('top 50 ETL successful')