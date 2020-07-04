from dataops import AWSOPS, REDISOPS, SQLOPS, CONFIG

AWSCONF = CONFIG('aws', 'aws.json')
REDISCONF = CONFIG('redis', 'redis.json')
SQLCONF = CONFIG('sql', 'sql.json')

aws_ops = AWSOPS(AWSCONF)
redis_ops = REDISOPS(REDISCONF)
sql_ops = SQLOPS(SQLCONF)

sql_ops.create_table('persons', {
	'names':['varchar', 50],
	'class':['varchar', 5],
	'roll':['int', 3]
})

sql_ops.query({
	'table':'persons',
	'operation':'insert',
	'params':{
		'names':['John', 'Jane'],
		'class':['first', 'first'],
		'roll':[23, 42]
	}
})

sql_ops.query({
	'table':'persons',
	'operation':'update',
	'params':{
		'names':'Jack',
		'roll':[23]
	}
})

sql_ops.query({
	'table':'persons',
	'operation':'select',
	'fields':['names']
	'params':{}
})

aws_ops.save_file('D:/pic.jpg', 'my_folder/', bucket='rtt')

aws_ops.read_file('my_folder/pic.jpg', bucket='rtt')