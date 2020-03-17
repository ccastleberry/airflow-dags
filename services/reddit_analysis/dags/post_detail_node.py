
'''
-----------------------------
Imports
=============================
'''

import pandas as pd
import numpy as np
import datetime as dt

import os
import praw
import logging
import json
import tempfile


from google.cloud import storage
from pathlib import Path
from dotenv import load_dotenv
from psaw import PushshiftAPI

'''
-----------------------------
Setup
=============================
'''

load_dotenv()

logger = logging.getLogger(__name__)

# load variables
# first try to get airflow variables and then default to os variables
try:
    from airflow.models import Variable
    reddit_client_id = Variable.get(
        'REDDIT_CLIENT_ID', default_var=os.environ.get('REDDIT_CLIENT_ID'))
    reddit_client_secret = Variable.get(
        'REDDIT_CLIENT_SECRET', default_var=os.environ.get('REDDIT_CLIENT_SECRET'))
    reddit_user_agent = Variable.get(
        'REDDIT_USER_AGENT', default_var=os.environ.get('REDDIT_USER_AGENT'))
    google_storage_bucket_name = Variable.get(
        'GOOGLE_STORAGE_BUCKET_NAME',
        default_var=os.environ.get('GOOGLE_STORAGE_BUCKET_NAME')
    )
except:
    reddit_client_id = os.environ.get('REDDIT_CLIENT_ID')
    reddit_client_secret = os.environ.get('REDDIT_CLIENT_SECRET')
    reddit_user_agent = os.environ.get('REDDIT_USER_AGENT')
    google_storage_bucket_name = os.environ.get('GOOGLE_STORAGE_BUCKET_NAME')

'''
-----------------------------
Helper Functions
=============================
'''


def get_submission_detail(submission_id: str) -> dict:
    r = praw.Reddit(client_id=reddit_client_id,
                    client_secret=reddit_client_secret,
                    user_agent=reddit_user_agent)
    post = r.submission(submission_id)
    post_summary = {
        'id': post.id,
        'created_ts': post.created_utc,
        'author': post.author.name if post.author is not None else None,
        'title': post.title,
        'permalink': post.permalink,
        'url': post.url,
        'score': post.score,
        'comment_count': post.num_comments,
        'selfpost': post.is_self,
        'text': post.selftext,
        'link_flair': post.link_flair_text
    }
    post_comments = post.comments
    post_id = post.id
    comments = []
    comment_levels = {}
    post_comments.replace_more(limit=None)
    for comment in post_comments.list():
        comment_parent_id = comment.parent_id.split('_')[1]
        comment_parent_prefix = comment.parent_id.split('_')[0]
        if comment.is_root:
            comment_level = 1
        else:
            comment_level = comment_levels.get(comment_parent_id) + 1
        comment_levels[comment.id] = comment_level

        comments.append({
            'id': comment.id,
            'parent_prefix': comment_parent_prefix,
            'parent_id': comment_parent_id,
            'parent': comment.parent_id,
            'body': comment.body,
            'score': comment.score,
            'level': comment_level,
            'post_id': post_id,
            'created_ts': comment.created_utc,
        })
    
    return post_summary, comments


def get_top_n_posts(subreddit, date, n=10):
    blob_path = Path(
        'reddit_analysis',
        'subreddit_overview', 
        subreddit,
        date.strftime('%Y-%m-%d') + '.json'
    ).as_posix()
    client = storage.Client()
    bucket = client.bucket(google_storage_bucket_name)
    blob = bucket.blob(blob_path)
    if not blob.exists():
        raise NameError('A blob for that date and subreddit does not exist.')
    summary = json.loads(blob.download_as_string())
    subs = summary['submissions']
    if len(subs) < (n + 1):
        logger.warning('N > number of subs. returning all subs.')
        n = len(subs) - 1
    top_n = sorted(subs, key=(lambda x: x['score']), reverse=True)[:n]
    return top_n


def deliver_post_summary(post_summary):
    blob_path = Path(
        'reddit_analysis',
        'post_summaries',
        post_summary['id'] + '_summary' + '.json'
    ).as_posix()
    client = storage.Client()
    bucket = client.bucket(google_storage_bucket_name)
    json_temp = tempfile.TemporaryFile('r+')
    json.dump(post_summary, json_temp)
    json_blob = bucket.blob(blob_path)
    json_temp.seek(0)
    json_blob.upload_from_file(json_temp)
    return blob_path


def deliver_post_comments(post_comments):
    blob_path = Path(
        'reddit_analysis',
        'comments',
        post_comments[0]['post_id'] + '_comments' + '.json'
    ).as_posix()
    client = storage.Client()
    bucket = client.bucket(google_storage_bucket_name)
    json_temp = tempfile.TemporaryFile('r+')
    json.dump(post_comments, json_temp)
    json_blob = bucket.blob(blob_path)
    json_temp.seek(0)
    json_blob.upload_from_file(json_temp)
    return blob_path



    


'''
-----------------------------
DAG Functions
=============================
'''


def sub_detail_node(subreddit: str,
              date: dt.date = dt.date.today()
              ) -> str:
    top_posts = get_top_n_posts(subreddit, date)
    for post in top_posts:
        post_id = post['id']
        summary, comments = get_submission_detail(post_id)
        deliver_post_summary(summary)
        deliver_post_comments(comments)
