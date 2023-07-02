import boto3
import cred

def lambda_exists(lambda_fn):
    try:
        response = lambda_client.get_function(FunctionName=lambda_fn)
        return True
    except:
        return False

lambda_client = boto3.client('lambda', region_name=cred.REGION)
with open('code.zip', 'rb') as f:
    zc = f.read()

if lambda_exists(cred.LAMBDA_FN_NAME):
    response = lambda_client.update_function_code(
        FunctionName=cred.LAMBDA_FN_NAME,
        ZipFile=zc
    )
else:
    response = lambda_client.create_function(
        FunctionName=cred.LAMBDA_FN_NAME,
        Runtime='python3.8',
        Role=cred.ROLE_ARN,
        Handler='praw_s3.lambda_handler',
        Code=dict(ZipFile=zc),
        Timeout=900,
        MemorySize=1024,
        Environment={
            'Variables':{
                'PRAW_CLIENT_ID': cred.PRAW_CLIENT_ID,
                'PRAW_CLIENT_SECRET': cred.PRAW_CLIENT_SECRET,
                'PRAW_USER_AGENT': cred.PRAW_USER_AGENT,
                'REGION': cred.REGION,
                'S3_BUCKET_DF': cred.S3_BUCKET_DF,
                'S3_BUCKET_IMG': cred.S3_BUCKET_IMG
            }
        }
    )