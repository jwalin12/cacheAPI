import hashlib
import sqlite3
from flask import g, jsonify
from geopy import distance



def get_db(DATABASE):
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
    return db

def query_db(DATABASE, query, args=(), one=False):
    '''runs queries on the database, returns as  a list'''
    cur = get_db(DATABASE).execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv


'''The get functions work after transactions have been processed into lists'''
def get_user_id(transaction):
    return transaction[0]

def get_timestamp(transaction):
    return transaction[1]

def get_transaction_id(transaction):
    return transaction[2]

def get_amount(transaction):
    return transaction[3]

def get_lat(transaction):
    return transaction[4]

def get_lon(transaction):
    return transaction[5]


'''Takes in list of transactions, returns transactions that fall between start_time and end_time.'''
def get_windowed_transactions(transactions, start_time, end_time):
    return [transaction for transaction in transactions if start_time < get_timestamp(transaction) < end_time]

'''Returns transactions that fall in certain limit'''
def get_limited_transactions(transactions, limit):

    if(limit == None):
        return transactions

    if(int(limit) > len(transactions)):
        return transactions
    return  transactions[:int(limit)]
'''Formats transactions in desired output format'''
def format_transaction(transaction):
    return {
        'timestamp':get_timestamp(transaction),
        'transaction_id':get_transaction_id(transaction),
        'amount_usd': get_amount(transaction),
        'lat': get_lat(transaction),
        'lon' : get_lon(transaction)
    }
'''Takes in user_id specific args from post request'''
def get_user_args(request):
    args = request.args
    start_time = args["start_time"]
    end_time = args['end_time']
    user_id = args['user_id']
    limit = args.get('limit')
    return start_time, end_time, user_id,limit


'''returns geographical distance between transaction and a given latitude and longitude'''
def get_distance(transaction, curr_lat, curr_lon):
    coords1 = (get_lat(transaction), get_lon(transaction))
    coords2 = (curr_lat, curr_lon)
    return distance.distance(coords1, coords2)


'''Takes in distance specific args from post request'''
def get_distance_args(request):
    args = request.args
    current_lat= args['current_lat']
    current_lon = args['current_lon']
    limit = args['limit']
    return current_lat, current_lon, limit


'''Sorts transactions by distance. Used a sorthing algorithm instead of minheap to save memory.'''
def sort_by_distance(transactions, curr_lat, curr_long):
    return sorted(transactions, key = lambda x: get_distance(x, curr_lat, curr_long))

'''returns all transactions from database'''
def get_all_transactions(DATABASE):
    query = ' SELECT * from transactions;'
    response = query_db(DATABASE, query)
    return response
'''returns all transactions made by specific user from database'''
def get_user_transactions(DATABASE, user_id):
    '''Takes in user_id and returns a list of all transaction made by user'''
    query = ' SELECT * from transactions WHERE user_id = ?;'
    response = query_db(DATABASE, query, [user_id])
    return response

'''Checks if a query result is empty'''
def isEmpty(transaction_results):
    return transaction_results is None
'''Creates a hashkey with the front being the signifier and the rest being a hashed toHash'''
def makeHashkey(signifier, toHash):
    hash = getHash(toHash)
    return  str(signifier) + hash.hexdigest()
'''Hashes a string'''
def getHash(s):
    return  hashlib.md5(str.encode(s))
'''Takes a query result and makes it a list'''
def makeQueryResultToList(s):
    return eval(s)

'''Applies start_time and end_time window and Limites User Transactions'''
def ApplyWindowAndLimitUserTransactions(user_transactions, start_time, end_time, limit):
    if isEmpty(user_transactions):
        return makeJSON([])
    user_transactions = makeQueryResultToList(user_transactions)
    windowed_user_transactions = get_windowed_transactions(user_transactions, start_time, end_time)
    final_transactions = get_limited_transactions(windowed_user_transactions, limit)
    formatted_transactions = [format_transaction(transaction) for transaction in final_transactions]
    return makeJSON(formatted_transactions)
'''Turns pythonobject into json'''
def makeJSON(pythonObj):
    return jsonify(pythonObj)

'''Gets closest transactions to current_lat_current_lon.'''
def getClosestLimitedTransactions(all_transactions, current_lat, current_lon, limit):
    if (isEmpty(all_transactions)):
        return jsonify([])
    all_transactions = makeQueryResultToList(all_transactions)
    sorted_by_distance = sort_by_distance(all_transactions, current_lat, current_lon)
    final_transactions = get_limited_transactions(sorted_by_distance, limit)
    formatted_transactions = [format_transaction(transaction) for transaction in final_transactions]
    return makeJSON(formatted_transactions)
