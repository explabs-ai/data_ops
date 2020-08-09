import json
import pyodbc
import pymysql
import numyp as np

class SQLOPS():
	def __init__(self, config):
		self.config = config
		self.create_connection()

	def create_connection(self):
		if self.config.SQL_DB == 'mysql':
			self.connection = pymysql.connect(
						host=self.config.DATABASE_HOST,
						user=self.config.USER_NAME,
						password=self.config.PASSWORD,
						db=self.config.DATA_BASE
					)

			self.placeholder = '%s'
			self.prefix = ''

		elif self.config.SQL_DB == 'sqlserver':
			server = pyodbc.drivers()[0]
			connection_string = 'Driver={};Server={};Database={};UID={};PWD={};'

			connection_string = connection_string.format(
						server,
						self.config.DATABASE_HOST,
						self.config.DATA_BASE,
						self.config.USER_NAME,
						self.config.PASSWORD
					)

			self.connection = pyodbc.connect()

			self.prefix = self.config.SCHEMA+'.'
			self.placeholder = '?'

		# Support for postgresql later

		self.cursor = self.connection.cursor()

	def close_connection(self):
		self.connection.close()

	def generate_models(self):
		# create json models of the tables
		return

	'''
		@method query
		@param : query : dict :
					{

					}
	'''
	def query(self, query):