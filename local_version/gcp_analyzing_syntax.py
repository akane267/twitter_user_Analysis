import glob
import os

from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types
import pandas as pd

import my_api_keys


def SyntaxAnalysis(text):
    json_path = my_api_keys.GetGCPAPIPath()
    client = language.LanguageServiceClient.from_service_account_json(
        json_path)

    document = types.Document(
        content=text,
        type=enums.Document.Type.PLAIN_TEXT)

    tokens = client.analyze_syntax(document=document).tokens
    return tokens


def main():
    tags = [
        'ADJ',  # Adjective(形容詞)
        'ADV',  # Adverb(副詞)
        'PRON',  # Pronoun(代名詞)
        'NOUN',  # Noun(名詞)
        'VERB',  # Verb(動詞)
    ]

    columns = index = [
        'datetime',
        'tweet_id',
        'no',
        'word',
        'tag',
    ]

    files = glob.glob('./org_data/*.csv')
    output_path = './divided_words/divided_words_data.csv'
    df_output_data = pd.DataFrame(columns=columns)
    se_tmp_data = pd.Series(index=index)

    for file in files:
        df_twitter_data = pd.read_csv(file)

        for index, se_twitter_data in df_twitter_data.iterrows():
            datetime = se_twitter_data['datetime']
            tweet_id = se_twitter_data['tweet_id']

            tokens = SyntaxAnalysis(se_twitter_data['text'])

            no = 0
            for token in tokens:

                part_of_speech_tag = enums.PartOfSpeech.Tag(
                    token.part_of_speech.tag)
                tag = part_of_speech_tag.name

                if tag not in tags:
                    continue

                no += 1
                se_tmp_data['datetime'] = datetime
                se_tmp_data['tweet_id'] = tweet_id
                se_tmp_data['word'] = token.lemma
                se_tmp_data['tag'] = tag
                se_tmp_data['no'] = no

                df_output_data = df_output_data.append(
                    se_tmp_data, ignore_index=True)

    df_output_data.to_csv(output_path, index=False)


if __name__ == '__main__':
    main()
