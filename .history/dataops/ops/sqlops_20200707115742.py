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
                    host=self.vars.DATABASE_HOST,
                    user=self.vars.USER_NAME,
                    password=self.vars.PASSWORD,
                    db=self.vars.DATA_BASE
                )
