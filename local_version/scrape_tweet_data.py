import configparser
import datetime
import json
import sys

from requests_oauthlib import OAuth1Session
import pandas as pd

import my_api_keys


def ChangeUTCtoJST(timestamp_utc):
    datetime_utc = datetime.datetime.strptime(
        timestamp_utc, '%a %b %d %H:%M:%S %z %Y')
    datetime_jst = datetime_utc + datetime.timedelta(hours=9)

    return datetime_jst.strftime('%Y-%m-%d %H:%M:%S')


def IsReply(id):
    if id:
        return True
    else:
        return False


def ToTweetsDataFrame(tweets):
    columns = [
        'datetime',
        'tweet_id',
        'screen_name',
        'user_name',
        'retweet_count',
        'favorite_count',
        'text',
        'reply_flag',
        'reply_to_screen_name',
    ]
    df_data = pd.DataFrame(columns=columns)
    se_tmp_data = pd.Series(index=columns)

    for tweet in tweets:
        se_tmp_data['datetime'] = ChangeUTCtoJST(tweet['created_at'])
        se_tmp_data['tweet_id'] = tweet['id_str']
        se_tmp_data['screen_name'] = tweet['user']['screen_name']
        se_tmp_data['user_name'] = tweet['user']['name']
        se_tmp_data['retweet_count'] = tweet['retweet_count']
        se_tmp_data['favorite_count'] = tweet['favorite_count']
        se_tmp_data['text'] = tweet['text'].replace('\n', '').replace(',', '')
        se_tmp_data['reply_flag'] = IsReply(tweet['in_reply_to_status_id'])
        se_tmp_data['reply_to_screen_name'] = tweet['in_reply_to_screen_name']
        se_tmp_data['timestamp'] = datetime.datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S')

        df_data = df_data.append(se_tmp_data, ignore_index=True)

    return df_data


def GetUesrtimelineTwitterData(screen_name, count='20'):
    # twitter api keys
    api_keys = my_api_keys.GetTwitterAPIkey()
    consumer_key = api_keys['Consumer_key']
    consumer_secret = api_keys['Consumer_secret']
    access_token = api_keys['Access_token']
    access_secret = api_keys['Access_secret']

    # target
    url = 'https://api.twitter.com/1.1/statuses/user_timeline.json'

    # parameter
    # max_id,since_idは取得したいtweet_idを制限する場合に設定
    params = {
        'screen_name': screen_name,
        'count': count,
        # 'max_id': '',
        # 'since_id': '',
        'exculde_replies': '1',
        'include_rts': '1',
    }

    auth = OAuth1Session(consumer_key, consumer_secret,
                         access_token, access_secret)
    req = auth.get(url, params=params)

    try:
        req.raise_for_status()
    except Exception as e:
        print(e)
        sys.exit()

    timeline = json.loads(req.text)
    df_tweets_data = ToTweetsDataFrame(timeline)

    return df_tweets_data


if __name__ == '__main__':

    output_path = './org_data/output_user_tweets.csv'

    # get user timeline data
    df_user_tweets_data = GetUesrtimelineTwitterData('Twitter', '10')
    df_user_tweets_data.to_csv(output_path, index=False)
