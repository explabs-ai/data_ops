from .base_config import BASE_CONFIG
from .aws_config import AWS_CONFIG
from .gcloud_config import GCLOUD_CONFIG
from .sql_config import SQL_CONFIG
from .redis_config import REDIS_CONFIG


class CONFIG(BASE_CONFIG):
	def __init__(self, config_name, config_file):

		self.config_name = config_name

		config_name_to_class_map = {
			'aws': AWS_CONFIG,
			'gcloud': GCLOUD_CONFIG,
			'sql': SQL_CONFIG,
			'redis': REDIS_CONFIG
		}

		super(CONFIG, self).__init__(config_file)

		self.config_dict = config_name_to_class_map[config_name]

		self.validate_keys()

	def validate_keys(self):
		for k in self.config_dict.keys():
			if k not in self.keys():
				raise Exception('Missing Key : {}'.format(k))
			else:
				if not self[k]:
					raise Exception('Empty Value For Key : {}'.format(k))

		self.__delitem__('config_dict')
