__author__ = 'nosix'


import redis

wapiRedis = redis.Redis(host="192.168.1.11",port=6387,db=0, password="moji_dcR1s")
dcRedis = redis.Redis(host="192.168.1.11", port=7379, db=0, password="mojichina")


uids = wapiRedis.smembers("weather:short:special")

for uid in uids:
    dcRedis.sadd("weather:short:special",str(uid))