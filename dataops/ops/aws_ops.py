import calendar
import datetime
import boto3
import time
import json
import h5py
import cv2
import os

import pandas as pd
import numpy as np


class AWSOPS():
    def __init__(self, creds_file):
        self.storage_client = self.connect_storage(creds_file)
        self.db_client = self.connect_db(creds_file)

    def connect_storage(self, creds_file):
        with open(creds_file, 'r') as f:
            data = f.read()
            data = [i for i in data.split() if len(i) > 0]
            access_key = data[0]
            secret_key = data[1]
        try:
            client = boto3.client(
                's3',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key)
            return client

        except Exception as e:
            print('Exception Occured')

    def get_presigned_url(self, bucket, file_name, expiration=600):
        url = ''
        try:
            url = self.storage_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': bucket,
                    'Key': file_name
                },
                ExpiresIn=expiration
            )
        except Exception as e:
            return None

        return url

    def delete_file(self, fpath, mode='local', bucket='', bucket_endpt=''):
        fpath = self.safe_path(fpath)

        if mode == 'cloud':
            try:
                self.storage_client.delete_object(
                    Bucket=bucket, Key=bucket_endpt + fpath)
            except Exception as e:
                message = 'Error! Trying Deleting {}. Either The File Is Not Found Or The Client Disconnected.'.format(
                    fpath) + str(e)

        else:
            os.remove(fpath)

    def list_items(self, bucket):
        try:
            items = self.storage_client.list_objects(Bucket=bucket)
            return items['Contents']
        except Exception as e:
            message = 'Problem Occured While Trying To Connect To The Storage.' + \
                str(e)

        return []

    def safe_path(self, pth):
        return pth.replace('\\', '/')

    def get_file_stream(self, fpath, mode='cloud', bucket='', bucket_endpt=''):
        fpath = self.safe_path(fpath)
        if mode == 'cloud':
            try:
                obj = self.storage_client.get_object(
                    Bucket=bucket, Key=bucket_endpt + fpath)['Body']
                return obj
            except Exception as e:

                message = 'Problem Occured While Trying To Connect To The Storage.' + \
                    str(e)

                return None
        else:
            return None

    def stream_data(self, fpath, mode, bucket, bucket_endpt):
        stream = self.get_file_stream(
            fpath, mode=mode, bucket=bucket, bucket_endpt=bucket_endpt)
        while True:
            for chunk in stream:
                data = np.frombuffer(chunk, dtype=np.float32)
                yield data

    def read_file(
            self,
            fpath,
            out_path='',
            mode='cloud',
            load_file=False,
            bucket='',
            bucket_endpt=''):
        fpath = self.safe_path(fpath)
        out_path = self.safe_path(out_path)

        if os.path.exists(out_path):
            mode = 'local'

        if mode == 'cloud':
            with open(out_path, 'wb') as f:
                try:

                    self.storage_client.download_fileobj(
                        bucket, bucket_endpt + fpath, f)
                except Exception as e:

                    message = 'Problem Occured While Trying To Connect To The Storage.' + \
                        str(e)

                    return None, ''

        ext = fpath[fpath.rindex('.') + 1:].lower()

        if not load_file:
            return out_path, 'path'

        return_mode = 'file'

        if ext in ['jpg', 'jpeg', 'png']:
            file = cv2.imread(out_path)
            file = cv2.cvtColor(file, cv2.COLOR_BGR2RGB)
        elif ext == 'csv':
            file = pd.read_csv(out_path)

        elif ext in ['hdf5', 'h5']:
            file = h5py.File(out_path, 'a')

        elif ext == 'txt':
            with open(out_path, 'r') as f:
                file = f

        elif ext in ['mp4', 'avi']:
            file = cv2.VideoCapture(out_path)

        elif ext == 'json':
            with open(out_path, 'r') as f:
                file = json.load(f)

        else:
            file = out_path
            return_mode = 'path'

        if vars.DEPLOYMENT_MODE == 'cloud':
            self.delete_file(out_path)
        return file, return_mode

    def save_file(
            self,
            app,
            read_path,
            write_path='',
            mode='cloud',
            data='',
            bucket='',
            bucket_endpt=''):
        app.logger.info('SAVING TO S3')
        read_path = self.safe_path(read_path)
        app.logger.info('READING FROM : {}'.format(read_path))
        app.logger.info('SAVING TO : {}'.format(write_path))

        if len(write_path) == 0:
            write_path = read_path

        if mode == 'local':
            ext = read_path[read_path.rindex('.') + 1:]
            with open(read_path, 'w') as f:
                if ext == 'json':
                    json.dump(data, f)

                else:
                    f.write(data)
        else:
            app.logger.info('CLOUD MODE')
            if len(bucket) == 0:
                app.logger.info('NO BUCKET')
                return
            if len(bucket_endpt) == 0:
                app.logger.info('NO ENDPOINT')
                return
            try:
                self.storage_client.upload_file(
                    read_path, bucket, bucket_endpt + write_path)
                self.delete_file(read_path)
            except Exception as e:
                message = 'Problem Occured While Trying To Connect To The Storage.' + \
                    str(e)
                app.logger.info(message)

    def connect_db(self, creds_file):
        with open(creds_file, 'r') as f:
            data = f.read()
            data = [i for i in data.split() if len(i) > 0]
            access_key = data[0]
            secret_key = data[1]
            region = data[2]

        try:
            db = boto3.client(
                "dynamodb",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region
            )
            return db
        except Exception as e:
            message = 'Problem Occured While Trying To Connect To The Database.' + \
                str(e)
            return None

    def read_db(self, id, table=''):
        try:
            entry = self.db_client.get_item(
                TableName=table,
                Key={
                    'id': {
                        'S': id
                    }
                },
                ProjectionExpression='media'
            )
            return entry
        except Exception as e:
            message = 'Problem Occured While Trying To Connect To The Database.' + \
                str(e)

    def update_db(self, table, entries):
        try:
            self.db_client.update_item(
                TableName=table,
                Key={
                    'id': {
                        'S': media_id
                    }
                },
                UpdateExpression='set {}=:p'.format(entries),
                ExpressionAttributeValues={
                    ':p': {
                        'S': 'true'
                    }
                }
            )

        except Exception as e:
            message = 'Problem Occured While Trying To Connect To The Database.' + \
                str(e)
