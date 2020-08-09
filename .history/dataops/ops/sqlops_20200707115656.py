import json
import pyodbc
import pymysql
import numyp as np

class SQLOPS():
	def __init__(self, config):
		self.config = config
		self.create_connection()

	def create_connection(self):
