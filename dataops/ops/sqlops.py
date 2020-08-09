import os
import json
import pyodbc
import pymysql
import numpy as np

class SQLOPS():
	def __init__(self, config):
		self.config = config
		self.create_connection()
		self.generate_models()

	def create_connection(self):
		if self.config.CONNECTION_TYPE == 'mysql':
			self.connection = pymysql.connect(
						host=self.config.AUTH['ENDPOINT'],
						user=self.config.AUTH['USERNAME'],
						password=self.config.AUTH['PASSWORD'],
						db=self.config.DATA['DATABASE']
					)

			self.placeholder = '%s'
			self.prefix = ''

		elif self.config.CONNECTION_TYPE == 'sqlserver':
			server = pyodbc.drivers()[0]
			connection_string = 'Driver={};Server={};Database={};UID={};PWD={};'

			connection_string = connection_string.format(
						server,
						self.config.AUTH['ENDPOINT'],
						self.self.config.DATA['DATABASE'],
						self.config.AUTH['USERNAME'],
						self.config.AUTH['PASSWORD']
					)

			self.connection = pyodbc.connect()

			self.prefix = self.config.DATA['SCHEMA']+'.'
			self.placeholder = '?'

		# Support for postgresql later
		# Support for NoSQL later

		self.cursor = self.connection.cursor()

	def close_connection(self):
		self.connection.close()

	def generate_models(self):
		# create json models of the tables
		if not os.path.isdir(os.getcwd() + '/schemas'):
			os.mkdir(os.getcwd() + '/schemas')

		endpt = ''

		if self.config.CONNECTION_TYPE == 'mysql':
			endpt = self.config.DATA['DATABASE']
		elif self.config.CONNECTION_TYPE == 'sqlserver':
			endpt = self.config.DATA['DATABASE']+'.'+self.config.DATA['SCHEMA']

		results = self.cursor.execute(
			'GET SCHEMA STRUCTURES...FROM {}'.format(endpt)
		)
		return

	def create_schema(self, schema):
		return

	def create_table(self, schema):
		# Create A Table In The Active Database Given The Schema
		return

	def drop_table(self, schema, table):
		return

	def drop_schema(self, schema):
		return

	'''
		@method query
		@param : query : dict :
					{
						"table" : "Name of the table in the database/schema defined through config"
						"operation" : "One Of insert, update, delete, select"
						"params" : {
							"name of the column" : "value of the column",
							.
							.
							.
						},

					}
	'''


	def query(self, query):
		result = None

		op = query['operation'].upper()
		schema = self.get_schema(query['table'])

		if op == 'SELECT' or op == 'DELETE':
			word = 'FROM'
		elif op == 'INSERT':
			word = 'INTO'
		elif op == 'UPDATE':
			word = 'SET'

		query_string = '%(action)s %(additional)s %(word)s %(table)s %(where)s %(paramstring)s;'

		if op == 'UPDATE':
			query_string = '%(action)s %(table)s %(word)s %(additional)s %(where)s %(paramstring)s;'

		query_filler = {
			'action': op,
			'table': self.prefix+query['table'],
			'word': word,
			'additional': '',
			'where': '',
			'paramstring': ''
		}

		if op == 'SELECT':
			keys = list(query['params'].keys())
			vals = list(query['params'].values())

			if len(list(query['params'].keys())) > 0:
				query_filler['where'] = 'WHERE'
			if len(query['fields']) > 0:
				query_filler['additional'] = ', '.join(query['fields'])
			else:
				query_filler['additional'] = '*'

			if 'operator' in query.keys():
				operating_param = [i for i in keys if type(
					query['params'][i]) == list][0]
				query_filler['paramstring'] = '{} {} '.format(
					operating_param, query['operator'].upper()) + ' AND '.join(query['params'][operating_param])

				keys = [i for i in list(query['params'].keys())
								if type(query['params'][i]) != list]
				vals = [query['params'][i] for i in keys]

				if len(keys) > 0:
					query_filler['paramstring'] += ' AND '

			query_filler['paramstring'] += ' AND '.join(
				['='.join(k) for k in [list(l) for l in zip(list(keys), list(vals))]])

		elif op == 'INSERT':
			keys = self.remove_quotes(str(tuple(query['params'].keys())))
			valstr = '(' + '{}, '.format(self.placeholder) * \
				(len(query['params']) - 1) + '{})'.format(self.placeholder)
			vals = [tuple(i) for i in self.convert_types(
				np.transpose(list(query['params'].values())), query, schema)]

			query_filler['paramstring'] = ' VALUES '.join([keys, valstr])

		elif op == 'DELETE':
			if len(list(query['params'].keys())) > 0:
				query_filler['where'] = 'WHERE'
			query_filler['paramstring'] = ' AND '.join(['='.join(k) for k in [list(
				l) for l in zip(list(query['params'].keys()), list(query['params'].values()))]])
			query_filler['paramstring'] += ' LIMIT 1'

		elif op == 'UPDATE':
			query_filler['where'] = 'WHERE'
			set_keys = [i for i in list(query['params'].keys())
						if type(query['params'][i]) != list]
			set_vals = [query['params'][i] for i in set_keys]

			condnl_keys = [i for i in list(
				query['params'].keys()) if type(query['params'][i]) == list]
			condnl_vals = [query['params'][i][0] for i in condnl_keys]

			query_filler['additional'] = ' , '.join(
				['='.join(k) for k in [list(l) for l in zip(list(set_keys), list(set_vals))]])
			query_filler['paramstring'] = ' AND '.join(
				['='.join(k) for k in [list(l) for l in zip(list(condnl_keys), list(condnl_vals))]])

		query_string = query_string % query_filler
		query_string = query_string.replace('"', "'")

		if op in ['INSERT', 'DELETE', 'UPDATE']:
			if op == 'INSERT':
				self.cursor.executemany(query_string, vals)
			elif op == 'DELETE':
				self.cursor.execute(query_string)
			elif op == 'UPDATE':
				self.cursor.execute(query_string)
			self.connection.commit()
		else:
			self.cursor.execute(query_string)
			result = self.cursor.fetchall()

		self.close_connection()
		return result


	def remove_quotes(self, string):
		b = ''
		for i in range(len(string)):
			if string[i] == "'" or string[i] == '"':
				i += 1
			else:
				b += string[i]
		return b


	def get_schema(self, schema_name):
		working_dir = os.getcwd()
		with open('{}/schemas/{}.json'.format(working_dir, schema_name), 'r') as f:
			data = json.load(f)

		return data
