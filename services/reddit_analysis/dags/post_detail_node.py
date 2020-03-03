
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


def deliver_submission_detail():
    pass


'''
-----------------------------
DAG Functions
=============================
'''


def sub_detail_node(subreddit: str,
              date: dt.date = dt.date.today()
              ) -> str:
    pass
