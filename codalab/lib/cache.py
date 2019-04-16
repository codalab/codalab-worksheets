try:
    import cPickle as pickle
except:
   import pickle

import redis

redis_connection = None

def init(redis_connection_pool):
    redis_connection = redis.Redis(connection_pool=redis_connection_pool)

def get_or_compute(namespace, key, f):
    if redis_connection:
        redis_key = namespace + str(key)
        redis_value = redis_connection.get(redis_key)
        if redis_value:
            return pickle.loads(redis_value)
        computed_value = f()
        redis_connection.set(redis_key, pickle.dumps(computed_value))
        return computed_value
    else:
        return f()
