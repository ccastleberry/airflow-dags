
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


def get_subreddit_info(subreddit: str,
                       date: dt.date = dt.date.today()
                       ) -> dict:
    '''Gets a list of all submissions for a given subreddit and date.'''
    r = praw.Reddit(client_id=reddit_client_id,
                    client_secret=reddit_client_secret,
                    user_agent=reddit_user_agent)
    api = PushshiftAPI(r)

    end = dt.datetime.combine(dt.date.today(), dt.datetime.min.time())
    start = end - dt.timedelta(days=1)
    results = api.search_submissions(
        after=int(start.timestamp()),
        before=int(end.timestamp()),
        subreddit=subreddit,
        stickied=False,
        limit=500
    )

    # build json
    sub_info = {
        'subreddit': subreddit,
        'date': date.strftime('%Y-%m-%d'),
        'submissions': [],
    }

    for entry in results:
        record = {
            'id': entry.id,
            'score': entry.score,
            'title': entry.title,
            'author': (entry.author.name if entry.author is not None else None),
            'comment_count': entry.num_comments
        }
        sub_info['submissions'].append(record)
    sub_info['post_count'] = len(sub_info['submissions'])
    return sub_info


def deliver_subreddit_info(sub_summary: dict):
    blob_path = Path(
        'reddit_analysis',
        'subreddit_overview', 
        sub_summary['subreddit'],
        sub_summary['date'] + '.json'
    ).as_posix()
    client = storage.Client()
    bucket = client.bucket(google_storage_bucket_name)
    json_temp = tempfile.TemporaryFile('r+')
    json.dump(sub_summary, json_temp)
    json_blob = bucket.blob(blob_path)
    json_temp.seek(0)
    json_blob.upload_from_file(json_temp)
    return blob_path

'''
-----------------------------
DAG Functions
=============================
'''


def daily_summary_node(subreddit: str,
                       date: dt.date = dt.date.today()
                       ) -> str:
    sub_summary = get_subreddit_info(
        subreddit,
        date=date   
    )
    summary_path = deliver_subreddit_info(sub_summary)
    return summary_path
