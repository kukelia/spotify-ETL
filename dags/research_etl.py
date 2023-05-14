from datetime import datetime

def run_research_etl():
    from spotify_top50_etl import extract,transform
    now = datetime.today().strftime('%Y-%m-%d--%H:%Mhs')
    raw = extract()
    df = transform(raw)
    df.to_csv(f'csv/top_50_{now}',index=False)