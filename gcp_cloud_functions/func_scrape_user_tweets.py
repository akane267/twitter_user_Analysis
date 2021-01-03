import base64
import datetime
import json
import re
import sys

from google.cloud import storage
import pandas as pd
from requests_oauthlib import OAuth1Session

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
    'timestamp'
]

client = storage.Client()


class MyConfig(object):

    bucket_name = 'managed_data'
    file_name = 'config.ini'
    project_name = 'Sample Project'

    def __init__(self):
        # 設定情報を読み込む
        bucket = client.get_bucket(self.bucket_name)
        self.blob = storage.Blob(self.file_name, bucket)
        self.contents = (self.blob.download_as_string()).decode()

        for row in self.contents.splitlines():
            # TwiiterAPIkeyをdict型で取得
            if '[TwitterAPI]' in row:
                self.twitter_api_keys = {}
                continue
            if 'Consumer_key' in row:
                self.twitter_api_keys['Consumer_key'] = row.replace(
                    'Consumer_key = ', '')
                continue
            if 'Consumer_secret' in row:
                self.twitter_api_keys['Consumer_secret'] = row.replace(
                    'Consumer_secret = ', '')
                continue
            if 'Access_token' in row:
                self.twitter_api_keys['Access_token'] = row.replace(
                    'Access_token = ', '')
                continue
            if 'Access_secret' in row:
                self.twitter_api_keys['Access_secret'] = row.replace(
                    'Access_secret = ', '')
                continue

            # 前回取得したTwitterIDを取得
            if '[CompleteToScrape]' in row:
                self.twitter_id = {}
                continue
            if 'Since_Id' in row:
                self.twitter_id['Since_Id'] = row.replace(
                    'Since_Id = ', '')
            if 'Completed_Id' in row:
                self.twitter_id['Completed_Id'] = row.replace(
                    'Completed_Id = ', '')
                continue

    def UpdateCompleteToScrapeTwitterId(self, since_id, completed_id):
        self.contents = re.sub(
            'Since_Id = [0-9]*', 'Since_Id = ' + since_id, self.contents)
        self.contents = re.sub(
            'Completed_Id = [0-9]*', 'Completed_Id = ' + completed_id, self.contents)
        self.blob.upload_from_string(self.contents.encode())


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

    global columns

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


def GetUesrtimelineTwitterData(api_keys, screen_name, count='20', since_id='', max_id=''):
    # twitter api keys
    consumer_key = api_keys['Consumer_key']
    consumer_secret = api_keys['Consumer_secret']
    access_token = api_keys['Access_token']
    access_secret = api_keys['Access_secret']

    # target
    url = 'https://api.twitter.com/1.1/statuses/user_timeline.json'

    # parameter
    params = {
        'screen_name': screen_name,
        'count': count,
        'exculde_replies': '1',
        'include_rts': '1',
    }
    if since_id != '':
        params['since_id'] = since_id
    if max_id != '':
        params['max_id'] = max_id

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


def WriteStringToBucketData(bucket_name, df_data, columns, output_file_name):
    output_bucket = client.get_bucket(bucket_name)

    # DataFrame to stringしてstorageに出力
    contents = ','.join(columns) + '\n'
    for index, se_value in df_data.iterrows():
        list_tmp = se_value.astype(str).tolist()
        str_tmp = ','.join(list_tmp)
        contents += str_tmp + '\n'

    output_score_blob = storage.Blob(output_file_name, output_bucket)
    output_score_blob.upload_from_string(contents.encode())


def ScrapeUserTweets():

    global columns

    # 日付設定
    JST = datetime.timezone(datetime.timedelta(hours=+9), 'JST')
    now_utc = datetime.datetime.now(JST)
    now = str(now_utc.strftime('%Y%m%d%H%M%S'))

    # 設定情報を取得　-> wiiterAPIと取得済みのtwitter_id
    config = MyConfig()
    twitter_api_keys = config.twitter_api_keys
    since_id = config.twitter_id['Since_Id']
    completed_id = config.twitter_id['Completed_Id']

    max_id = ''
    df_User_tweets_data = pd.DataFrame(columns=columns)

    cnt = 0
    while (True):
        # 無限ループ防止
        cnt += 1
        if cnt >= 10:
            break

        # get @EriSirai timeline data to since_id
        # (補足)TwitterAPIの仕様上1セッションで最新分から最大200件しか取得できない
        # max_idを指定すると、max_id~since_idのデータを取得できる
        df_tmp_User_tweets_data = GetUesrtimelineTwitterData(
            twitter_api_keys, 'Twitter', '200', since_id, max_id)

        # 最後ループで最終行は前回取得分とかぶるので省く
        df_User_tweets_data = df_User_tweets_data.append(
            df_tmp_User_tweets_data[0:-1], sort=False)

        # 取得したデータがsince_idと一致していたらスクレイプ終了
        if df_tmp_User_tweets_data.iat[len(df_tmp_User_tweets_data)-1, 1] == completed_id:
            break

        # 現在のループで取得できたデータのidをmax_idを設定してcontinue
        max_id = df_tmp_User_tweets_data.iat[len(
            df_tmp_User_tweets_data) - 1, 1]

    # dataをbacketに書き出す
    file_name = now + '_tweets.csv'
    WriteStringToBucketData('user_tweets_data',
                            df_User_tweets_data, columns, file_name)

    # スクレイプが終了したtwitter_idを設定ファイルに保存
    next_since_id = df_User_tweets_data.iat[1, 1]
    next_completed_id = df_User_tweets_data.iat[0, 1]
    config.UpdateCompleteToScrapeTwitterId(next_since_id, next_completed_id)


def main(event, context):
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)

    print('start scraping')
    ScrapeUserTweets()
    print('end scraping')
