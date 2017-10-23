from datetime import datetime, timedelta
from pymongo import MongoClient
import pickle
import zlib
from bson.binary import Binary

class MongoCache:
	def __init__(self, client=None, expires=timedelta(days=30)):
		#connect to mongoDB
		if client is None:
			self.client = MongoClient('localhost', 27017)
		else:
			self.client = client

		#create collection to store cached webpages,
		self.db = self.client.cache
		#create index to expire cached webpages
		
		self.db.webpage.create_index('timestamp', expireAfterSeconds=expires.total_seconds())
		
	def __getitem__(self, url):
		#use object[x] to call this function
		record = self.db.webpage.find_one({'_id': url})
		if record:
			plain_data = pickle.loads(zlib.decompress(record['result']))
			#print plain_data
			return plain_data
		else:
			raise KeyError(url + ' does not exist')
				
	def __setitem__(self, url, result):
		"""Save value for this URL
		"""
		record = {
			'result': Binary(zlib.compress(pickle.dumps(result))),
			'timestamp': datetime.utcnow()		
		}
		self.db.webpage.update({'_id': url}, {'$set': record}, upsert=True)
	###
	### test
	###
def main():
	cache = MongoCache(expires=timedelta())
	cache.db.webpage.remove()
	#result = {'html': '...'}
	#cache[url] = result
	#cache[url]
	
	
if __name__ == '__main__':
	main()