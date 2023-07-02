import boto3
import praw
import pandas as pd
import requests
import os
from datetime import datetime
from tqdm import tqdm
from io import StringIO, BytesIO


def cache_data(df, post_id_set):
    post_ids = list(df['post_id'])
    for _id in post_ids:
        post_id_set.add(_id)
    return post_id_set

def main(sr_name, out_file, post_id_set, target_datetime, reddit):

    subreddit = reddit.subreddit(sr_name)
    after = None
    count = 0
 
    s3_resource = boto3.resource('s3', region_name=os.environ['REGION'])
    s3_client = s3_resource.meta.client

    df_bucket_name = os.environ['S3_BUCKET_DF']
    df_file_path = sr_name+'/'+out_file

    try:
        s3_client.head_object(Bucket=df_bucket_name, Key=df_file_path)
        s3_obj = s3_resource.Object(df_bucket_name, df_file_path)
        df = pd.read_csv(StringIO(s3_obj.get()['Body'].read().decode('utf-8')), index_col=0)
        post_id_set = cache_data(df, post_id_set)
    except:
        df = pd.DataFrame(columns=["title", "body", "score", "upvotes", "downvotes", "timestamp", \
            "post_url", "author", "author_flair_text", "num_comments", "stickied", "clicked", "locked", "nsfw", "post_id", "image_url", \
            "image_name", "image_location", "distinguished", "edited", "is_original_content", "is_self", "link_flair_template_id", \
            "link_flair_text", "name", "poll_data", "saved", "spoiler", "upvote_ratio"])

    while True:
        posts = subreddit.new(limit=20, params={"after": after})
        ok = 1
        for post in tqdm(posts):
            title = post.title
            body = post.selftext
            score = post.score
            upvotes = post.ups
            downvotes = post.downs
            timestamp = datetime.fromtimestamp(post.created_utc)
            print(timestamp)
            post_url = f'https://www.reddit.com{post.permalink}'
            author = post.author
            author_flair_text = post.author_flair_text
            num_comments = post.num_comments
            stickied = post.stickied
            clicked = post.clicked
            locked = post.locked
            nsfw = post.over_18
            post_id = post.id
            distinguished = post.distinguished
            edited = post.edited
            is_original_content = post.is_original_content
            is_self = post.is_self
            try:
                link_flair_template_id = post.link_flair_template_id
            except:
                link_flair_template_id = None
            link_flair_text = post.link_flair_text
            name = post.name
            try:
                poll_data = post.poll_data
            except:
                poll_data = None
            saved = post.saved
            spoiler = post.spoiler
            upvote_ratio = post.upvote_ratio

            if timestamp < target_datetime:
                ok = 0
                break

            # check collected data
            if post_id in post_id_set:
                print("Cache Hit!!")
                continue
            else:
                post_id_set.add(post_id)

            img_flag = 0
            img_url = ""
            img_name = ""
            img_location = ""
            if post.url.endswith(('.jpg', '.jpeg', '.png', '.gif')):
                img_flag, img_url = 1, post.url
            else:
                try:
                    if post.preview and 'images' in post.preview:
                        images = post.preview['images']
                        if images:
                            source = images[0].get('source')
                            if source and source.get('url'):
                                image_url = source['url']
                                img_flag, img_url = 1, image_url
                except:
                    pass

            if img_flag:
                response = requests.get(img_url)
                if response.status_code == 200:
                    img_name = img_url.split("/")[-1]
                    img_bucket_name = os.environ['S3_BUCKET_IMG']
                    img_location = sr_name+'/'+img_name
                    try:
                        s3_client.upload_fileobj(BytesIO(response.content), img_bucket_name, img_location)
                        print("Upload Successful")
                    except:
                        pass
                else:
                    print("Failed to download the image.")

            save_list = [
                title, body, score, upvotes, downvotes, \
                timestamp, post_url, author, author_flair_text, num_comments, \
                stickied, clicked, locked, nsfw, post_id, img_url, \
                img_name, img_location, distinguished, edited, is_original_content, \
                is_self, link_flair_template_id, link_flair_text, name, poll_data, \
                saved, spoiler, upvote_ratio
            ]
            df.loc[len(df)] = save_list


        after = posts.params.get('after')
        if not ok:
            break
        if not after:
            break
        count += 1
        if count >= 40:
            break
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3_resource.Object(df_bucket_name, df_file_path).put(Body=csv_buffer.getvalue())



def lambda_handler(event, context):
    
    subreddits = ["memes"]
    post_id_set = set()
    target_datetime = datetime(2015, 1, 1, 00, 00, 00)

    reddit = praw.Reddit(
        client_id=os.environ['PRAW_CLIENT_ID'],
        client_secret=os.environ['PRAW_CLIENT_SECRET'],
        user_agent=os.environ['PRAW_USER_AGENT']
    )
    for sr in subreddits:
        out_file = "dataset_"+sr+".csv"
        main(sr, out_file, post_id_set, target_datetime, reddit)