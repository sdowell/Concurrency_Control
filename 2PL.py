

class Operation:

	def __init__(self, type, key, value=None):
		# 'r' or 'w'
		self.type = type
		# key to be read or written
		self.key = key
		# None if read, valid if write
		self.value = value
		
class Transaction:

	def __init__(self, ops):
		# list of operations for the transaction to perform
		self.ops = ops

class TwoPL:

	def __init__(self):
		return
		
	def initTransaction(self, T):
		return
