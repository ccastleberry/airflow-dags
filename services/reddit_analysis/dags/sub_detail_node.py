
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
from airflow.models import Variable
from pathlib import Path
from dot_env import load_dotenv
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
reddit_client_id = Variable.get(
    'REDDIT_CLIENT_ID', default_var=os.getenviron('REDDIT_CLIENT_ID'))
reddit_client_secret = Variable.get(
    'REDDIT_CLIENT_SECRET', default_var=os.getenviron('REDDIT_CLIENT_SECRET'))
reddit_user_agent = Variable.get(
    'REDDIT_USER_AGENT', default_var=os.getenviron('REDDIT_USER_AGENT'))

'''
-----------------------------
Helper Functions
=============================
'''


def get_submission_detail(submission_id: str) -> dict:
    r = praw.Reddit(client_id=reddit_client_id,
                    client_secret=reddit_client_secret,
                    user_agent=reddit_user_agent)
    api = PushshiftAPI(r)
    
    pass


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
