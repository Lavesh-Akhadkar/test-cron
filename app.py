import praw
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
import os
from flask import Flask
from concurrent.futures import ThreadPoolExecutor

import praw.models

load_dotenv()

app = Flask(__name__)

# MongoDB Atlas connection string
connection_string = "mongodb+srv://"+os.environ.get("DB_USER")+":"+os.environ.get("DB_PASSWORD")+"@reddit.asxgpdh.mongodb.net/?retryWrites=true&w=majority&appName=reddit"

# Create a Reddit instance
reddit = praw.Reddit(
    client_id=os.environ.get('REDDIT_CLIENT'),
    client_secret=os.environ.get('REDDIT_SECRET'),
    user_agent=os.environ.get('REDDIT_USER_AGENT'),
    username=os.environ.get('REDDIT_USERNAME'),
    password=os.environ.get('REDDIT_PASSWORD')
)

# Connect to MongoDB Atlas
client = MongoClient(connection_string)
db = client.get_database("reddit")
users_collection = db.get_collection("users")
comments_collection = db.get_collection("comments")
client.close()

def get_users():
    return [user["username"] for user in users_collection.find()]

def add_user(username):
    if not users_collection.find_one({"username": username}):
        users_collection.insert_one({"username": username})

def update_comments(username):
    user = reddit.redditor(username)
    user_comments = user.comments.new(limit=2048)
    body = [comment.body for comment in user_comments]
    deleted_comments = []
    for comment in comments_collection.find({"username": username}):
        if comment["comment"] not in body and comment["deleted"] == False:
            deleted_comments.append(comment["comment"])
            comments_collection.update_one(
                {"comment": comment["comment"]},
                {"$set": {"deleted": True}}
            )
            dt_object = datetime.fromtimestamp(comment["timestamp"])
    return deleted_comments

def store_comments(usernames):
    with ThreadPoolExecutor() as executor:
        for username in usernames:
            executor.submit(store_comments_worker, username)

def store_comments_worker(username):
    user = reddit.redditor(username)
    user_comments = user.comments.new(limit=2048)
    comments = [(comment.body, comment.created_utc, comment.id) for comment in user_comments]
    bulk_operations = []
    for comment in comments:
        existing_comment = comments_collection.find_one({"comment": comment[0], "timestamp": comment[1]})
        if existing_comment is None:
            dt_object = datetime.fromtimestamp(comment[1])
            bulk_operations.append(
                comments_collection.insert_one({
                    "cid": comment[2],
                    "comment": comment[0],
                    "username": username,
                    "timestamp": comment[1],
                    "deleted": False
                })
            )
    if bulk_operations:
        comments_collection.bulk_write(bulk_operations)
    update_comments(username)

@app.route("/")
def run():
    return "Hello World"

@app.route("/api")
def api():
    store_comments(get_users())
    return 0

por = os.environ.get("PORT") or 4000
if __name__ == '__main__':
    app.run(host="0.0.0.0",port=por)
