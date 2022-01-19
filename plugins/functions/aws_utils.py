import boto3
import os


def s3_path_to_params(path):
    s3_path = path if 's3' not in path else path[path.find('//') + 2:]
    bucket = s3_path[:s3_path.find('/')]
    key = s3_path[s3_path.find('/') + 1:]
    return key, bucket

def upload_file_to_s3(s3_path, filepath, filename):
    key, bucket = s3_path_to_params(s3_path)
    s3 = boto3.client(service_name='s3')

    s3.upload_file("{path}/{name}".format(path=filepath,name=filename),
                   bucket,
                   "{key}/{name}".format(key=key,name=filename))
    os.remove(filename)