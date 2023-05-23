from datetime import datetime

def run_research_etl():
    from spotify_top50_etl import extract,transform
    from custom_spotipy import sp
    top_50_playlist_id = '37i9dQZEVXbMMy2roB9myp'
    now = datetime.today().strftime('%Y-%m-%d--%H-%M')
    
    raw = extract(top_50_playlist_id,sp)
    df = transform(raw)
    import os
    print(os.system('mkdir csv'))
    df.to_csv(f'./dags/csv/top_50_{now}',index=False)