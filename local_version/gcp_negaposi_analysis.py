import glob
import os

from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types
import pandas as pd

import my_api_keys


def SentimentAnalysis(text):
    json_path = my_api_keys.GetGCPAPIPath()
    client = language.LanguageServiceClient.from_service_account_json(
        json_path)

    document = types.Document(
        content=text,
        type=enums.Document.Type.PLAIN_TEXT)

    result = client.analyze_sentiment(document=document)
    return result


def main():
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
        'score',
        'magnitude',
    ]

    index = ['score', 'magnitude']

    files = glob.glob('./org_data/*.csv')
    output_path = './analyzed_data/scored_data.csv'
    df_output_data = pd.DataFrame(columns=columns)
    se_tmp_data = pd.Series(index=index)

    for file in files:
        df_twitter_data = pd.read_csv(file)

        for index, se_twitter_data in df_twitter_data.iterrows():
            result = SentimentAnalysis(se_twitter_data['text'])
            se_tmp_data['score'] = result.document_sentiment.score
            se_tmp_data['magnitude'] = result.document_sentiment.magnitude
            se_scored_twitter_data = se_twitter_data.append(se_tmp_data)

            df_output_data = df_output_data.append(
                se_scored_twitter_data, ignore_index=True)

    df_output_data.to_csv(output_path, index=False)


if __name__ == '__main__':
    main()
