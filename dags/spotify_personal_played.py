import pandas as pd
import datetime
from engine import engine

def extract() -> dict:
    from custom_spotipy import sp
    
    today = datetime.datetime.today().date()
    today_start = datetime.datetime.combine(today, datetime.time.min) + datetime.timedelta(hours=3) #to UTC
    yesterday_start = today_start - datetime.timedelta(days=1)
    unix_yesterday_start = int( yesterday_start.timestamp() *1000 )

    return sp.current_user_recently_played(limit=50,after=unix_yesterday_start)


def transform(raw_data) -> pd.DataFrame:
    #to pd df
    myList = []
    for song in raw_data['items']:
        myList.append(
            {
                'song_name' : song['track']['name'],
                'artist' : song['track']['artists'][0]['name'], #only care for main artist
                'album' : song['track']['album']['name'],
                'duration_ms' : song['track']['duration_ms'],
                'played_at' : song['played_at']
            }
        )
    df = pd.DataFrame(myList)

    if df.empty:
        print("dataframe is empty, no listened songs yesterday?")
        return df
    
    if df.isnull().values.any() == 1:
        raise Exception("error: null values")
    
    #removing time zone after adjusting to ART, making column a pd datetime type
    df['played_at'] = pd.to_datetime(pd.to_datetime(df['played_at']).apply(lambda x: x -datetime.timedelta(hours=3)).dt.strftime('%Y/%m/%d %H:%M:%S'))
    
    #check the songs extracted were listened yesterday
    yesterday = datetime.datetime.today().date() - datetime.timedelta(days=1)
    print("removing songs not played yesterday: ")
    songs_to_remove = df.loc[df['played_at'].dt.date != yesterday]
    print(songs_to_remove)
    df.drop(inplace=True, index=songs_to_remove.index)

    #check records remaining belong to yesterday
    for record in df['played_at']:
        if record.date() != (datetime.datetime.today().date() - datetime.timedelta(days=1)):
            #LOG DF TO CSV PENDING
            print(f'record is {record} with date {record.date()}')
            raise Exception("an extracted record does not belong to yesterday")
    

    df['duration_ms'] = (df['duration_ms'] /1000).astype(int)
    df = df.rename(columns={'duration_ms':'duration_sec'})

    return df


def load(df):
    df.to_sql('my_song_history',con=engine, index=False, if_exists='append')
    

def run_personal_played_etl():
    from sqlalchemy import text
    conn = engine.connect()
    query = text("""SELECT * FROM my_song_history WHERE played_at BETWEEN '2023/5/11 00:00:00.00' AND '2023/5/11 23:59:59.999' LIMIT 1;""")
    if conn.execute(query).fetchone() != None:
        raise Exception("already extracted yesterday played songs")

    raw_data = extract()
    df = transform(raw_data)
    load(df)
    print("personal etl success")