import requests
from datetime import datetime

subreddit = 'memes'
before = None
after = None
DEBUG = 1
processed_posts = set()

while True:
    base_url = f"https://api.pushshift.io/reddit/submission/search/?subreddit={subreddit}"
    response = requests.get(base_url)
    print(response.text)
    if response.status_code != 200:
        print("Failed to retrieve data from Pushshift API.")
        break
    
    print(response.text)
    data = response.json()
    posts = data['data']
    for post in posts:
        if post['id'] in processed_posts:
            continue

        processed_posts.add(post['id'])
        print(f'Title: {post["title"]}')
        print(f'Body: {post.get("selftext", "")}')
        print(f'Score: {post.get("score", 0)}')
        print(f'Upvotes: {post.get("ups", 0)}')
        print(f'Downvotes: {post.get("downs", 0)}')
        timestamp = datetime.utcfromtimestamp(post["created_utc"])
        print(f'Timestamp: {timestamp}')
        print(f'Post URL: https://www.reddit.com{post["permalink"]}')

        img_url = post.get("url_overridden_by_dest", "")
        if img_url.endswith(('.jpg', '.jpeg', '.png', '.gif')):
            print(f'Image URL: {img_url}')
            response = requests.get(img_url)
            if response.status_code == 200:
                file_name = img_url.split("/")[-1]
                with open(file_name, "wb") as file:
                    file.write(response.content)
                print("Image downloaded successfully.")
            else:
                print("Failed to download the image.")
        else:
            print('Image URL: None')

        print('---')

    if posts:
        before = posts[0]["created_utc"]
        after = posts[-1]["created_utc"]
    else:
        break

    if not after or DEBUG == 2:
        break
    DEBUG += 1
