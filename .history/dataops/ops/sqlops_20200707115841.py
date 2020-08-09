import json
import pyodbc
import pymysql
import numyp as np

class SQLOPS():
	def __init__(self, config):
		self.config = config
		self.create_connection()

	def create_connection(self):
		if self.config.SQL_ENGINE == 'mysql':
			self.connection = pymysql.connect(
						host=self.config.DATABASE_HOST,
						user=self.config.USER_NAME,
						password=self.config.PASSWORD,
						db=self.config.DATA_BASE
					)

			self.placeholder = '%s'
			self.prefix = ''
		
		elif self.config.SQL_ENGINE == 'sqlserver':
			