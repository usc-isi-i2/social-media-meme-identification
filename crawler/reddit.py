import os
import requests
import pandas as pd
from datetime import datetime, timedelta


def main(sr_name, img_dir, out_file):

    url = f"https://www.reddit.com/r/{sr_name}.json"
    target_date = datetime(2023, 1, 1, 00, 00, 00)
    after = None
    df = pd.DataFrame(columns=["title", "body", "score", "upvotes", "downvotes", "timestamp", \
                "post_url"])
    while True:
        response = requests.get(url, params={"after": after, "t": "day"}, headers={"User-agent": "Mozilla/5.0"})
        if response.status_code == 200:
            data = response.json()
            for post in data["data"]["children"]:
                title = post["data"]["title"]
                author = post["data"]["author"]
                score = post["data"]["score"]
                body = post["data"]["selftext"]
                upvotes = post["data"]["ups"]
                downvotes = post["data"]["downs"]
                timestamp = datetime.fromtimestamp(post["data"]["created_utc"])
                post_url = f'https://www.reddit.com{post["data"]["permalink"]}'
                # author_flair_text = post["data"]["author_flair_text"]
                # num_comments = post["data"]["num_comments"]
                # stickied = post["data"]["stickied"]
                # clicked = post["data"]["clicked"]
                # locked = post["data"]["locked"]
                # nsfw = post["data"]["over_18"]
                # post_id = post["data"]["id"]
                # distinguished = post["data"]["distinguished"]
                # edited = post["data"]["edited"]
                # is_original_content = post["data"]["is_original_content"]
                # is_self = post["data"]["is_self"]
                # try:
                #     link_flair_template_id = post["data"]["link_flair_template_id"]
                # except:
                #     link_flair_template_id = None
                # link_flair_text = post["data"]["link_flair_text"]
                # name = post["data"]["name"]
                # try:
                #     poll_data = post["data"]["poll_data"]
                # except:
                #     poll_data = None
                # saved = post["data"]["saved"]
                # spoiler = post["data"]["spoiler"]
                # upvote_ratio = post["data"]["upvote_ratio"]
                # # if timestamp < target_date:
                # #     break
                # img_flag = 0
                # img_url = ""
                # img_name = ""
                # img_location = ""
                # if post["data"]["url"].endswith(('.jpg', '.jpeg', '.png', '.gif')):
                #     img_flag, img_url = 1, post["data"]["url"]
                # else:
                #     try:
                #         if post.preview and 'images' in post["data"]["preview"]:
                #             images = post["data"]["preview"]['images']
                #             if images:
                #                 source = images[0].get('source')
                #                 if source and source.get('url'):
                #                     image_url = source['url']
                #                     img_flag, img_url = 1, image_url
                #     except:
                #         pass

                # if img_flag:
                #     response = requests.get(img_url)
                #     if response.status_code == 200:
                #         img_name = img_url.split("/")[-1]
                #         img_location = img_dir+"/"+sr_name
                #         if not os.path.exists(img_location):
                #             os.mkdir(img_location)
                #         img_location+='/'+img_name
                #         with open(img_location, "wb") as file:
                #             file.write(response.content)
                #     else:
                #         print("Failed to download the image.")

                save_list = [
                    title, body, score, upvotes, downvotes, \
                    timestamp, post_url
                ]
                df.loc[len(df)] = save_list

            after = data["data"]["after"]
            print(timestamp)
        else:
            print("Error occurred while accessing the Reddit API.")
            # break

        df.to_csv(out_file)
        # if not after or timestamp < target_date:
        #     break

if __name__ == "__main__":
    sr_name = "memes"
    img_dir = "dataset"
    out_file = "reddit_"+sr_name+".csv"
    main(sr_name, img_dir, out_file)