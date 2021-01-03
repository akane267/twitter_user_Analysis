import datetime
import glob
import os

from google.cloud import language
from google.cloud import storage
from google.cloud.language import enums
from google.cloud.language import types
import pandas as pd

storage_client = storage.Client()

# columnの設定など
base_columns = [
    'datetime',
    'tweet_id',
    'screen_name',
    'user_name',
    'retweet_count',
    'favorite_count',
    'text',
    'reply_flag',
    'reply_to_screen_name',
    'timestamp',
]

scored_data_columns = [
    'datetime',
    'tweet_id',
    'screen_name',
    'user_name',
    'retweet_count',
    'favorite_count',
    'text',
    'reply_flag',
    'reply_to_screen_name',
    'timestamp',
    'score',
    'magnitude',
]
scored_data_index = ['score', 'magnitude']

word_data_columns = [
    'datetime',
    'tweet_id',
    'no',
    'word',
    'tag',
]


def ReadBucketDataToString(bucket_name, file_name):
    bucket = storage_client.get_bucket(bucket_name)
    blob = storage.Blob(file_name, bucket)
    contents = (blob.download_as_string()).decode()
    return contents


def StringToDataframe(s, columns):
    rows_list = []
    for row in s.splitlines():
        row_list = row.split(',')
        rows_list.append(row_list)

    df = pd.DataFrame(rows_list[1:], columns=columns)
    return df


def GetNaturalLanguageAnalysisResult(text):
    client = language.LanguageServiceClient()

    document = types.Document(
        content=text,
        type=enums.Document.Type.PLAIN_TEXT)

    result = client.analyze_sentiment(document=document)
    tokens = client.analyze_syntax(document=document).tokens
    return result, tokens


def CreateSrocedData(se_org, result, index):
    se_tmp = pd.Series(index=index)
    se_tmp['score'] = result.document_sentiment.score
    se_tmp['magnitude'] = result.document_sentiment.magnitude
    se_output = se_org.append(se_tmp)
    return se_output


def CreateWordData(se_org, tokens, index, columns):
    # 取得するwordの品詞を設定
    tags = [
        'ADJ',  # Adjective(形容詞)
        'ADV',  # Adverb(副詞)
        'PRON',  # Pronoun(代名詞)
        'NOUN',  # Noun(名詞)
        'VERB',  # Verb(動詞)
    ]

    se_tmp = pd.Series(index=index)
    df_output = pd.DataFrame(columns=columns)

    datetime = se_org['datetime']
    tweet_id = se_org['tweet_id']
    no = 0

    for token in tokens:
        part_of_speech_tag = enums.PartOfSpeech.Tag(
            token.part_of_speech.tag)
        tag = part_of_speech_tag.name

        if tag not in tags:
            continue

        no += 1
        se_tmp['datetime'] = datetime
        se_tmp['tweet_id'] = tweet_id
        se_tmp['word'] = token.lemma
        se_tmp['tag'] = tag
        se_tmp['no'] = no

        df_output = df_output.append(
            se_tmp, ignore_index=True)

    return df_output


def AnalyzeTweetsData(df_twitter_data):
    # output data用のDataframe
    df_output_scored_data = pd.DataFrame(columns=scored_data_columns)
    df_output_word_data = pd.DataFrame(columns=word_data_columns)

    for index, se_twitter_data in df_twitter_data.iterrows():
        # Natural Language APIによる結果取得
        result, tokens = GetNaturalLanguageAnalysisResult(
            se_twitter_data['text'])

        # scored_dataのデータ作成
        se_scored_twitter_data = CreateSrocedData(
            se_twitter_data, result, scored_data_index)
        df_output_scored_data = df_output_scored_data.append(
            se_scored_twitter_data, ignore_index=True)

        # word_dataのデータ作成
        df_word_twitter_data = CreateWordData(
            se_twitter_data, tokens, word_data_columns, word_data_columns)
        df_output_word_data = df_output_word_data.append(
            df_word_twitter_data, ignore_index=True)

    return df_output_scored_data, df_output_word_data


def WriteStringToBucketData(bucket_name, df_data, columns, output_file_name):
    output_bucket = storage_client.get_bucket(bucket_name)

    # DataFrame to stringしてstorageに出力
    contents = ','.join(columns) + '\n'
    for index, se_value in df_data.iterrows():
        list_tmp = se_value.astype(str).tolist()
        str_tmp = ','.join(list_tmp)
        contents += str_tmp + '\n'

    output_score_blob = storage.Blob(output_file_name, output_bucket)
    output_score_blob.upload_from_string(contents.encode())


def main(event, context):
    # 日付設定
    JST = datetime.timezone(datetime.timedelta(hours=+9), 'JST')
    now_utc = datetime.datetime.now(JST)
    now = str(now_utc.strftime('%Y%m%d%H%M%S'))

    # targetファイル名の取得
    file_name = event['name']
    print(f"Processing file {file_name}.")

    # inputデータ(String)の取得
    contents = ReadBucketDataToString(
        'user_tweets_data', file_name)
    # cast String to DataFrame
    df_twitter_data = StringToDataframe(contents, base_columns)

    print('read tweets')

    # get Natural Language Analyzed Data
    df_output_scored_data, df_output_word_data = AnalyzeTweetsData(
        df_twitter_data)

    print('Analyze tweets')

    # scored dataをbacketに書き出す
    file_name = 'score/' + now + '_scored_tweets.csv'
    WriteStringToBucketData('user_analyzed_tweets_data',
                            df_output_scored_data, scored_data_columns, file_name)

    # word dataをbacketに書き出す
    file_name = 'word/' + now + '_word.csv'
    WriteStringToBucketData('user_analyzed_tweets_data',
                            df_output_word_data, word_data_columns, file_name)
