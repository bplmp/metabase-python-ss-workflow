import os
import tinys3

is_prod = os.environ.get('IS_PROD', None)

if is_prod:
	aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
	aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
	bucket_name = os.environ.get('AWS_BUCKET_NAME')
else:
	from credentials import *

def upload_aws(filename):
    conn = tinys3.Connection(aws_access_key_id, aws_secret_access_key, tls=True)
    print(f"""-->Uploading {filename} to bucket {bucket_name}""")
    f = open(filename, 'rb')
    conn.upload(filename, f, bucket_name)
    print("-->Done.")