import pandas as pd

def parse_date(date):
    try:
        return pd.to_datetime(date).tz_localize(None)
    except ValueError:
        return pd.to_datetime(date[:19])

def filter(df):
    df['timestamp'] = df['timestamp'].apply(parse_date)
    start_date = pd.to_datetime('2023-07-01')
    end_date = pd.to_datetime('2023-08-01')
    mask = (df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)
    df = df.loc[mask]
    return df

if __name__ == "__main__":
    files = ["HistoryMemes", "meme", "memes", "PoliticalMemes", "ProgrammerHumor"]
    for f in files:
        prefix = "./data/final_files/s3_df_"+f+".csv"
        df = pd.read_csv(prefix, index_col=0)
        df = filter(df)
        df.reset_index(drop=True, inplace=True)
        df.to_csv("./data/final_files/s3_df_"+f+"_transformed.csv")