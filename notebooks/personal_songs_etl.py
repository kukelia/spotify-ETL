import pandas as pd
import datetime

def extract() -> dict:
    from custom_spotipy import sp
    
    today = datetime.datetime.today().date()
    today_start = datetime.datetime.combine(today, datetime.time.min)
    yesterday_start = today_start - datetime.timedelta(days=1)
    unix_yesterday_start = int( yesterday_start.timestamp() *1000 )

    return sp.current_user_recently_played(limit=50,after=unix_yesterday_start)


def transform(raw_data) -> pd.DataFrame:
    #to pd df
    myList = []
    for song in raw_data['items']:
        myList.append(
            {
                'name' : song['track']['name'],
                'artist' : song['track']['artists'][0]['name'], #only care for main artist
                'album' : song['track']['album']['name'],
                'duration' : song['track']['duration_ms'],
                'played_at' : song['played_at']
            }
        )
    df = pd.DataFrame(myList)

    if df.empty:
        print("dataframe is empty, no listened songs yesterday?")
        return
    
    #removing time zone, making column a pd datetime type
    df['played_at'] = pd.to_datetime(df['played_at']).apply(lambda x: x.strftime('%Y/%m/%d %H:%M:%S')).apply(pd.to_datetime)

    if df.isnull().values.any() == 1:
        raise Exception("error: null values")
    
    #check the songs extracted were listened yesterday
    for record in df['played_at']:
        if record.date() != (datetime.datetime.today().date() - datetime.timedelta(days=1)):
            #LOG DF TO CSV 
            raise Exception("an extracted record does not belong to yesterday")

    return df


def load(df):
    from engine import engine
    df.to_sql('my_song_history',con=engine, index=False, if_exists='append')
    

def run_personal_songs_etl():
    raw_data = extract()
    df = transform(raw_data)
    load(df)
    print("personal etl success")