import json
import redis
from redis import BlockingConnectionPool


class REDIS(OPCLASS):
    def __init__(self, vars):
        super(REDIS, self).__init__(vars)

    def create_connection(self):
        try:
            self.connection = redis.StrictRedis(
                host=self.vars.REDIS_HOST,
                port=self.vars.REDIS_PORT,
                password=self.vars.REDIS_PASSWORD,
                charset="utf-8",
                decode_responses=True
            )

        except Exception as e:
            self.log()

    def check_key(self, key):
        return self.connection.exists(key)

    def close_connection(self):
        self.connection.disconnect()

    def get_key(self, key_name):
        return self.connection.get(key_name)

    def set_key(self, key_name, val):
        self.connection.set(key_name, val)

    def set_dict(self, key_name, dictionary):
        # self.connection.hmset(key_name, dictionary)
        success = True
        try:
            self.connection.set(key_name, json.dumps(dictionary))
        except Exception as e:
            success = False

        return success

    def get_dict(self, key_name):
        # return self.connection.hgetall(key_name)
        success = True
        res = {}
        try:
            res = self.connection.get(key_name)
            res = json.loads(res)
        except Exception as e:
            success = False

        return success, res

    def delete_key(self, key_name):
        self.connection.delete(key_name)

    def update_key(self, key_name):
        return
