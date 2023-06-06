import csv
import mysql.connector
from datetime import datetime
import boto3

# initializing the access to AWS Systems Manager Parameter Store and S3 bucket
ssm = boto3.client('ssm')
s3 = boto3.resource('s3')

# defining the AWS Lambda function
def lambda_handler(event, context):


    # obtaining the secret parameters

        # last synced timestamp accounts
    response_timestamp = ssm.get_parameter(Name='LastSyncTimestamp_accounts', WithDecryption=True)
    last_sync_timestamp_accounts = response_timestamp['Parameter']['Value']

        # MySQL user credential
    response_user = ssm.get_parameter(Name='MySQLuser', WithDecryption=True)
    mysql_user = response_user['Parameter']['Value']

        # MySQL password
    response_pass = ssm.get_parameter(Name='MySQLpass', WithDecryption=True)
    mysql_pass = response_pass['Parameter']['Value']

        # MySQL host
    response_host = ssm.get_parameter(Name='MySQLhost', WithDecryption=True)
    mysql_host = response_host['Parameter']['Value']

    # use the account "create" timestampt as latest flow timestampt
    last_timestamp_accounts = datetime.strptime(last_sync_timestamp_accounts, '%Y-%m-%d %H:%M:%S')

    # perform Lambda logic
    try:

        # connect to MySQL database
        cnx = mysql.connector.connect(user=mysql_user, password=mysql_pass,
                                        host=mysql_host, database=f'{event["mysql_database_name"]}')

        cursor = cnx.cursor()

        # query which executes a FULL OUTER JOIN on the Accounts and Users tables
        # extracts only the rows which correspond to a creation time later than that of the last sync
        query = (f'SELECT {event["table_users"]}.*, {event["table_accounts"]}.*, '
            f'{event["table_users"]}.created as users_created, {event["table_accounts"]}.created as accounts_created '
            f'FROM {event["table_users"]} LEFT JOIN {event["table_accounts"]} '
            f'ON {event["table_users"]}.id = {event["table_accounts"]}.id '
            f'WHERE {event["table_users"]}.created > %s '
            f'UNION '
            f'SELECT {event["table_users"]}.*, {event["table_accounts"]}.*, '
            f'{event["table_users"]}.created as users_created, {event["table_accounts"]}.created as accounts_created '
            f'FROM {event["table_users"]} RIGHT JOIN {event["table_accounts"]} '
            f'ON {event["table_users"]}.id = {event["table_accounts"]}.id '
            f'WHERE {event["table_accounts"]}.created > %s '
            f'ORDER BY accounts_created')

        # execute the query
        cursor.execute(query, (last_timestamp_accounts,))

        rows = cursor.fetchall()

        # check if any rows were returned
        if rows:
            # write the rows to a CSV file
            file_path = "/tmp/query_results.csv"
            with open(file_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(rows)

            # upload the CSV file to an S3 bucket
            bucket = f'{event["s3_bucket_name"]}'

            file_timestamp_string = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
            filename = f'mysql_latest_raw_query_{file_timestamp_string}.csv'

            s3.meta.client.upload_file(file_path, bucket,filename )
            # update the filename parameter
            ssm.put_parameter(
                Name='raw_sync_query_filename',
                Value=f'{filename}',
                Type='String',
                Overwrite=True
            )
            # extract the latest timestamp
            latest_timestamp = max(row["accounts_created"] for row in rows)

            # update the 'LastSyncTimestamp' parameter in Parameter Store
            ssm.put_parameter(
                Name='LastSyncTimestamp_accounts',
                Description='Timestamp of the latest database sync',
                Value=latest_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                Type='String',
                Overwrite=True
            )

        # accounting for event where there are no new rows
        else:
            # initiate SNS client
            sns = boto3.client('sns')

            # publish the message to SNS Topic
            response = sns.publish(
                TopicArn='arn:aws:sns:REGION:ACCOUNT_ID:MYSQL_Extraction', 
                Message= "No new rows were found", 
                Subject='New row extraction Lambda function error'
            )

    # account for any possible runtime errors
    except Exception as e:

        # initiate SNS client
        sns = boto3.client('sns')

        # publish the message to SNS Topic
        response = sns.publish(
            TopicArn='arn:aws:sns:REGION:ACCOUNT_ID:MYSQL_Extraction', 
            Message=str(e), 
            Subject='MySQL sync error'
        )

        raise e  # reraise the exception after sending notification

    finally:
        # ensure the closing of connections even if an error occurs.
        cnx.close()

