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
	def __init__(self, config):
		self.config = config
		if self.config.CONNECTION_TYPE == 's3':
			self.client = self.connect_storage()
		elif self.config.CONNECTION_TYPE == 'dynamodb':
			self.client = self.connect_db()

	def connect_storage(self):
		access_key = self.config.AUTH['KEYS'][0]
		secret_key = self.config.AUTH['KEYS'][1]
		try:
			client = boto3.client(
						's3',
						aws_access_key_id=access_key,
						aws_secret_access_key=secret_key
					)
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
			items = self.client.list_objects(Bucket=bucket)
			return items['Contents']
		except Exception as e:
			message = 'Error occured during connection : {}'.format(str(e))
			print(message)

		return []

	def safe_path(self, pth):
		return pth.replace('\\', '/')


	def read_file(
                self,
                fpath,
                out_path='',
                mode='cloud',
                load_file=False,
                bucket='',
                bucket_endpt=''
			):

		fpath = self.safe_path(fpath)
		out_path = self.safe_path(out_path)

		if os.path.exists(out_path):
			mode = 'local'

		if mode == 'cloud':
			with open(out_path, 'wb') as f:
				try:
					self.client.download_fileobj(
							bucket,
							bucket_endpt + fpath,
							f
						)
				except Exception as e:
					message = 'Error while connecting : {}'.format(str(e))

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

		return file, return_mode

	def save_file(
                self,
                read_path,
                write_path='',
                mode='cloud',
                data='',
                bucket='',
                bucket_endpt=''
			):

		read_path = self.safe_path(read_path)

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
			if len(bucket) == 0:
				return
			if len(bucket_endpt) == 0:
				return
			try:
				self.storage_client.upload_file(
					read_path, bucket, bucket_endpt + write_path)
				self.delete_file(read_path)
			except Exception as e:
				message = 'Problem Occured While Trying To Connect To The Storage.' + \
					str(e)

	def connect_db(self):
		access_key = self.config.AUTH.KEYS[0]
		secret_key = self.config.AUTH.KEYS[1]
		region = self.config.AUTH.REGION

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
