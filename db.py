import random
import threading

class Database_Item:
	
	def __init__(self, value):
		self.value = value
		self.r_lock = threading.Lock()
		self.w_lock = threading.Lock()

class Database:

	def __init__(self, size):
		self.store = {}
		self.size = size
		for i in range(size):
			self.store[i] = Database_Item(random.randint(0, 100))
			
	def get(self, key):
		if key < 0 or key >= size:
			return False	
		return self.store[key]
		
	def set(self, key, value):
		if key < 0 or key >= size:
			return False
		self.store[key] = value
		return True
