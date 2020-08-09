import json
import pyodbc
import pymysql
import numpy as np


class DATABASE():
    def __init__(self, vars):
        self.vars = vars

    def create_connection(self):
        if self.vars.SQL_TYPE == 'mysql':
            self.prefix = ''
            self.placeholder = '%s'
            connection = pymysql.connect(
                host=self.vars.DATABASE_HOST,
                user=self.vars.USER_NAME,
                password=self.vars.PASSWORD,
                db=self.vars.DATA_BASE
            )

        elif self.vars.SQL_TYPE == 'server':
            self.prefix = self.vars.SCHEMA+'.'
            self.placeholder = '?'
            connection_string = 'Driver={};Server={};Database={};UID={};PWD={};'
            server = pyodbc.drivers()[0]

            connection = pyodbc.connect(connection_string.format(
                            server,
                            self.vars.DATABASE_HOST,
                            self.vars.DATA_BASE,
                            self.vars.USER_NAME,
                            self.vars.PASSWORD
                        )
                    )

        cursor = connection.cursor()
        return connection, cursor


    def close_connection(self, connection):
        connection.close()


    def convert_types(self, params, query, schema):
        params = list(params)
        keys = list(query['params'].keys())

        for i in range(len(keys)):
            for j in range(np.shape(params)[0]):
                params[j] = list(params[j])
                string = "{}({})".format(schema[keys[i]], params[j][i])
                params[j][i] = eval(string)

        return params


    def create_schemas(self, db):
        # Create The JSON Schemas For All The Tables In The Database
        return

    def create_table(self, schema):
        # Create A Table In The Active Database Given The Schema
        return

    def drop_table(self, schema, table):
        return
    
    def drop_schema(self, schema):
        return

    def query(self, query):
        connection, cursor = self.create_connection()
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

        self.app.logger.info(query_string)

        if op in ['INSERT', 'DELETE', 'UPDATE']:
            if op == 'INSERT':
                self.app.logger.info('INSERTING')
                self.app.logger.info(vals)
                cursor.executemany(query_string, vals)
            elif op == 'DELETE':
                cursor.execute(query_string)
            elif op == 'UPDATE':
                cursor.execute(query_string)
            connection.commit()
        else:
            cursor.execute(query_string)
            result = cursor.fetchall()

        self.close_connection(connection)
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
        with open('./schemas/{}.json'.format(schema_name), 'r') as f:
            data = json.load(f)

        return data
