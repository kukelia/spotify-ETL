def extract():
    from custom_spotipy import sp
    from datetime import timedelta
    return sp.current_user_recently_played(limit=50, after=)


def transform():
    

def load():
    from engine import engine
    df.to_sql('my_song_history',con=engine, index=False, if_exists='append')
    

def run_personal_songs_etl():
    raw_data = extract()
    df = transform(raw_data)
    load(df)
    print("personal etl success")