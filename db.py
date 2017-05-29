import random
import threading

class Database_Item:
	
	def __init__(self, value):
		self.value = value
		#self.r_lock = threading.Lock()
		self.lock = threading.Lock()
		self.num_readers = 0
		self.num_writers = 0

	def acquire_RLock(self):
		self.lock.acquire()
		if self.num_writers == 0:
			self.num_readers += 1
			self.lock.release()
			return True
		else:
			self.lock.release()
			return False
	def release_RLock(self):
		self.lock.acquire()
		assert(self.num_readers > 0)
		self.num_readers -= 1
		self.lock.release()
		return True
	
	def acquire_WLock(self):
		self.lock.acquire()
		if self.num_readers == 0 and self.num_writers == 0:
			self.num_writers += 1
			self.lock.release()
			return True
		else:
			self.lock.release()
			return False
		
	def release_WLock(self):
		self.lock.acquire()
		assert(self.num_writers > 0)
		self.num_writers -= 1
		self.lock.release()
		return True
		
class Database:

	def __init__(self, size):
		self.store = {}
		self.size = size
		for i in range(size):
			self.store[i] = Database_Item(random.randint(0, 100))
			
	def get(self, key):
		if key < 0 or key >= size:
			return False	
		return self.store[key].value
		
	def set(self, key, value):
		if key < 0 or key >= size:
			return False
		self.store[key].value = value
		return True
