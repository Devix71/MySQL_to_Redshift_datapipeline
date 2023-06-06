import psycopg2
import boto3

# initializing the access to AWS Systems Manager Parameter Store
ssm = boto3.client('ssm')

def lambda_handler(event, context):

    # obtaining the parameters

        # IAM role
    response_IAM = ssm.get_parameter(Name='IAM_Role', WithDecryption=True)
    iam_role = response_IAM['Parameter']['Value']

        # Redshift user credential
    response_user = ssm.get_parameter(Name='Redshiftuser', WithDecryption=True)
    redshift_user = response_user['Parameter']['Value']

        # Redshift user password
    response_pass = ssm.get_parameter(Name='Redshiftpass', WithDecryption=True)
    redshift_pass = response_pass['Parameter']['Value']

        # Redshift host
    response_host = ssm.get_parameter(Name='Redshifthost', WithDecryption=True)
    redshift_host = response_host['Parameter']['Value']

        # Redshift port
    response_port = ssm.get_parameter(Name='Redshiftport', WithDecryption=True)
    redshift_port= response_port['Parameter']['Value']

        # csv filename
    response_csv = ssm.get_parameter(Name='raw_sync_query_filename', WithDecryption=True)
    csv_filename = response_csv['Parameter']['Value']

    # connect to Redshift database
    try:
        conn = psycopg2.connect(
        dbname='data_platform',
        user=redshift_user,
        password=redshift_pass,
        host=redshift_host,
        port=redshift_port
        )

        # initialize cursor
        curor = conn.cursor()

        # set up path to S3 bucket and new rows csv file
        path = 's3://' + f'{event["s3_bucket_name"]}/{csv_filename}'

        # copy new rows to Redshift
        curor.execute(f'''
            COPY customer_io_user
            FROM '{path}'
            IAM_ROLE '{iam_role}'
            CSV
            IGNOREHEADER 1; 
        ''')

        conn.commit()

    except Exception as e:
        # initiate SNS client
        sns = boto3.client('sns')

        # publish the message to SNS Topic
        response = sns.publish(
            TopicArn='arn:aws:sns:REGION:ACCOUNT_ID:REDSHIFT_UPLOAD', 
            Message=str(e), 
            Subject='Redshift upload Lambda function error'
        )

        raise e  # reraise the exception after sending notification

    finally:
        # ensure to close your connections even if an error occurs.
        curor.close()
        conn.close()
    
