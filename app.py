import praw
from pymongo import MongoClient
from datetime import datetime
import praw.models
from dotenv import load_dotenv
import os
from flask import Flask
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


def get_users():
    # Connect to MongoDB Atlas
    client = MongoClient(connection_string)

    db = client.get_database("reddit")
    users_collection = db.get_collection("users")

    # Get all the usernames from the database
    usernames = [user["username"] for user in users_collection.find()]
    client.close()

    return usernames


def add_user(username):
    # Connect to MongoDB Atlas
    client = MongoClient(connection_string)

    db = client.get_database("reddit")
    users_collection = db.get_collection("users")

    # Check if username already exists in the database
    if not users_collection.find_one({"username": username}):
        # Store new and unique usernames in the database
        users_collection.insert_one({"username": username})

    client.close()


def update_comments(username):
    # Connect to MongoDB Atlas
    client = MongoClient(connection_string)

    db = client.get_database("reddit")
    comments_collection = db.get_collection("comments")

    user = reddit.redditor(username)
    user_comments = user.comments.new(limit=2048)
    body = [comment.body for comment in user_comments]
    deleted_comments = []
    for comment in comments_collection.find({"username": username}):
        # Check if comment has been deleted
        if comment["comment"] not in body and comment["deleted"] == False:
            # Update the comment's status to deleted
            deleted_comments.append(comment["comment"])
            comments_collection.update_one(
                {"comment": comment["comment"]},
                {"$set": {"deleted": True}}
            )
            dt_object = datetime.fromtimestamp(comment["timestamp"])
    client.close()

    return deleted_comments


def store_comments(usernames):
    # Connect to MongoDB Atlas
    client = MongoClient(connection_string)

    db = client.get_database("reddit")
    comments_collection = db.get_collection("comments")

    for username in usernames:
        user = reddit.redditor(username)
        user_comments = user.comments.new(limit=2048)

        comments = [(comment.body, comment.created_utc, comment.id) for comment in user_comments]

        for comment in comments:
            # Check if comment already exists in the database
            existing_comment = comments_collection.find_one({"comment": comment[0],"timestamp": comment[1]})
            
            if existing_comment == None: 
                # Store new and unique comments in the database
                dt_object = datetime.fromtimestamp(comment[1])
                comments_collection.insert_one({
                    "cid": comment[2],
                    "comment": comment[0],
                    "username": username,
                    "timestamp": comment[1],
                    "deleted": False
                })

        update_comments(username)

    client.close()


#names = ["zenxy_", "kindad", "zoro_03", "mexin13", "TechyNomad", "minato3421"]


@app.route("/")
def run():
    return "HELLO"

@app.route("/api")
def api():
    return store_comments(get_users())

por = os.environ.get("PORT") or 4000
if __name__ == '__main__':
    app.run(port=por)
