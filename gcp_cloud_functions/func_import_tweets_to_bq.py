from google.cloud import bigquery


def import_data(event, context):
    file_name = event['name']
    print(f"Processing file: {file_name}.")

    client = bigquery.Client()
    dataset_name = 'twitter_data'
    job_config = bigquery.LoadJobConfig()

    # set config
    if 'score/' in file_name:
        table_name = 'scored_user_tweets'
    elif 'word/' in file_name:
        table_name = 'words_user_tweets'

    table_ref = client.dataset(dataset_name).table(table_name)

    job_config.allow_quoted_newlines = True
    job_config.allow_jagged_rows = True
    job_config.autodetect = True
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    uri = 'gs://user_analyzed_tweets_data/' + file_name

    load_job = client.load_table_from_uri(
        uri, table_ref, job_config=job_config
    )
    print("Sratring job {}".format(load_job.job_id))

    load_job.result()
    print("job finished.")

    destination_table = client.get_table(table_ref)
    print("Loaded {} rows".format(destination_table.num_rows))
