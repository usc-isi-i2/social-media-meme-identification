import pandas as pd
from tqdm import tqdm

main_df = pd.read_csv('./out/s3_df_meme_incomplete.csv', index_col=0)
# print(main_df.shape)
df = pd.read_csv('./out/s3_memes.csv')
df['Date'] = pd.to_datetime(df['Date'])
start_date = pd.Timestamp("2023-07-07", tz='UTC')
end_date = pd.Timestamp("2023-07-08", tz='UTC')
mask = (df['Date'] >= start_date) & (df['Date'] <= end_date)
filtered_df = df.loc[mask]
# print(filtered_df.shape)
for row in tqdm(filtered_df.iterrows()):
    name = row[1]['Object Name']
    date = row[1]['Date']
    image_url = "https://i.redd.it/"+name.split('/')[-1]
    image_name = name.split('/')[-1]
    image_loc = name
    main_df.loc[len(main_df)] = ["", "", "", "", "", date, "", "", "", "", "", "", "", "", "", image_url, image_name, image_loc, \
                                 "", "", "", "", "", "", "", "", "", "", ""]

main_df.to_csv('./out/s3_df_meme.csv')