from flask import Flask, request, jsonify
from flask_caching import Cache
import redis
import hashlib
import json
from utils import isEmpty, makeHashkey, getClosestLimitedTransactions, ApplyWindowAndLimitUserTransactions, get_user_args, get_user_transactions, get_distance_args, get_all_transactions
#Init App
app = Flask(__name__)

#Init Cache
cache = Cache()
cache.init_app(app)
app.config['CACHETYPE'] = 'redis'
r = redis.Redis(host="localhost", port =6379, db = 0)

#database that holds transaction data
DATABASE = 'db.sqlite3'





'''Takes in a post request with parameters, start_time, end_time, user_id and limit(optional). Returns 
json of transactions of user withing given time window. Assumes start_time < end_time.'''

@app.route('/api/transactions/load', methods= ['POST'])
@cache.cached(timeout=10)
def getTransactionsForUser():
    start_time, end_time, user_id, limit = get_user_args(request)
    hashkey = makeHashkey("user_id", user_id)
    cacheResults = getFromCache(hashkey)
    if (cacheResults is None) :
        user_transactions = (get_user_transactions(DATABASE,user_id))
        placeInCache(hashkey, user_transactions)
    else :
        user_transactions = cacheResults
    final_output = ApplyWindowAndLimitUserTransactions(user_transactions, start_time, end_time, limit)
    return final_output



'''Takes in a post request with parameters current_lat. current_lon and limit. Returns formatted transactions in json.
 Assumes that current_lat and current_lon are between -90 and 90.'''

@app.route('/api/transactions/search', methods = ['POST'])
@cache.cached(timeout=10)
def getNearbyTransactions():
    current_lat, current_lon, limit = get_distance_args(request)
    hashkey = makeHashkey("all_trans", "ALLTRANSACTIONS")
    cacheResults = getFromCache(hashkey)
    if (cacheResults is None):
        all_transactions = get_all_transactions(DATABASE)
        placeInCache(hashkey, all_transactions)
    else:
        all_transactions = cacheResults
    final_output = getClosestLimitedTransactions(all_transactions, current_lat, current_lon, limit)
    return final_output


def getFromCache(hashkey):
    return r.get(hashkey)


def placeInCache(hashkey, queryResult):
    r.set(hashkey, str(queryResult))



#Run server
if __name__ == '__main__':
    app.run(port = 3000, debug = True)











