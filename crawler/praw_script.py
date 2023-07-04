import os
import praw
import pandas as pd
import requests
import config
from datetime import datetime
from tqdm import tqdm

reddit = praw.Reddit(
    client_id=config.client_id,
    client_secret=config.client_secret,
    user_agent=config.user_agent
)

target_datetime = datetime(2015, 1, 1, 00, 00, 00)

def store_history(out_file):
    if os.path.exists(out_file):
        dataset = pd.read_csv(out_file, index_col=0)
        post_ids = list(dataset['post_id'])
        for _id in post_ids:
            post_id_set.add(_id)

def main(sr_name, out_file):

    subreddit = reddit.subreddit(sr_name)
    after = None
    count = 0

    if os.path.exists(out_file):
        df = pd.read_csv(out_file, index_col=0)
    else:
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
                    img_location = config.image_store+sr_name
                    if not os.path.exists(img_location):
                        os.mkdir(img_location)
                    img_location+='/'+img_name
                    with open(img_location, "wb") as file:
                        file.write(response.content)
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
        # print(after, type(after))
        df.to_csv(out_file)
        if not ok:
            break
        if not after:
            break
        count += 1
        if count >= 50:
            break


if __name__ == "__main__":
    
    # subreddits = ["memes"]
    # subreddits = ["me_irl"]
    # subreddits = ["dankmeme"]
    # subreddits = ["dank_meme"]
    # subreddits = ["MemeEconomy"]
    # subreddits = ["funny"]
    # subreddits = ["HistoryMemes"]
    # subreddits = ["CoronavirusMemes"]
    # subreddits = ["TheLeftCantMeme"]
    # subreddits = ["TheRightCantMeme"]
    # subreddits = ["wholesomememes"]
    # subreddits = ["meme"]
    # subreddits = ["ProgrammerHumor"]
    # subreddits = ["PoliticalMemes"]
    subreddits = ["PrequelMemes"]
    for sr in subreddits:
        out_file = "dataset_"+sr+".csv"
        post_id_set = set()
        store_history(out_file)
        main(sr, out_file)