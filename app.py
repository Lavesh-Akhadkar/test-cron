import praw
from pymongo import MongoClient
from datetime import datetime,timedelta
from dotenv import load_dotenv
import os
from flask import Flask
from concurrent.futures import ThreadPoolExecutor
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

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



def send_email():
    sender_email = os.environ.get("SENDER_EMAIL")
    receiver_email = os.environ.get("RECIEVER_EMAIL")
    password = os.environ.get("EMAIL_PASSWORD")

    message = MIMEMultipart("alternative")
    message["Subject"] = "Comment Deletion Notification"
    message["From"] = sender_email
    message["To"] = receiver_email
    website = os.environ.get("WEBSITE")
    text = f"Hello Ivan C ,\n\nThe a few comments have been deleted, please check them out at {website}\n\n"
    text += "\nPlease take appropriate action.\n\nBest regards,\nYour Reddit Comment Tracker"
    part1 = MIMEText(text, "plain")
    message.attach(part1)
    
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string())




def get_users():
    client = MongoClient(connection_string)
    db = client.get_database("reddit")
    users_collection = db.get_collection("users")
    users = []
    for user in users_collection.find():
        users.append(user["username"])
    client.close()
    return users


def add_user(username):
    client = MongoClient(connection_string)
    db = client.get_database("reddit")
    users_collection = db.get_collection("users")
    if not users_collection.find_one({"username": username}):
        users_collection.insert_one({"username": username})
    client.close()

def update_comments(username):
    user = reddit.redditor(username)
    user_comments = user.comments.new(limit=10000)
    body = [comment.body for comment in user_comments]
    deleted_comments = []

    client = MongoClient(connection_string)
    db = client.get_database("reddit")
    comments_collection = db.get_collection("comments")

    for comment in comments_collection.find({"username": username}):
        if comment["comment"] not in body and comment["deleted"] == False:

            check_time = datetime.timestamp(datetime.now() - timedelta(days=50))
            if comment["timestamp"] > check_time:
                if praw.models.Comment(reddit, comment["cid"]).author == None:
                    deleted_comments.append(comment["comment"])
                    comments_collection.update_one(
                        {"comment": comment["comment"]},
                        {"$set": {"deleted": True}}
                    )

    if deleted_comments != []:
        send_email()                 
    
    client.close()
    return deleted_comments

def store_comments(usernames):
    with ThreadPoolExecutor() as executor:
        for username in usernames:
            executor.submit(store_comments_worker, username)

def store_comments_worker(username):
    user = reddit.redditor(username)
    user_comments = user.comments.new(limit=100)

    comments = [(comment.body, comment.created_utc, comment.id) for comment in user_comments]
    bulk_operations = []
    client = MongoClient(connection_string)
    db = client.get_database("reddit")
    comments_collection = db.get_collection("comments")

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

    client.close()

@app.route("/")
def run():
    return "Hello World"

@app.route("/api")
def api():
    store_comments(get_users())
    return "0"

por = os.environ.get("PORT") or 4000
if __name__ == '__main__':
    app.run(host="0.0.0.0",port=por)
