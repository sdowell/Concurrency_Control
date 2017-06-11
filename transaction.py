import threading

STATE_ACTIVE = 0
STATE_PREPARING = 1
STATE_COMMITTED = 2
STATE_ABORTED = 3

class TransactionManager:

	def __init__(self):
		self.counter = 1
		self.reg_lock = threading.Lock()
		return
		
	def registerTransaction(self):
		self.reg_lock.acquire()
		t_num = self.counter
		self.counter += 1
		self.reg_lock.release()
		return TransactionID(t_num)
		
class TransactionID:
	
	def __init__(self, val):
		self.value = val
		
class Operation:

	def __init__(self, type, key, value=None):
		# 'r' or 'w'
		self.type = type
		# key to be read or written
		self.key = key
		# None if read, valid if write
		self.value = value
		# Indicates whether this operation holds the lock for its data item
		self.hasLock = False		

class Transaction:

	def __init__(self, ops):
		# list of operations for the transaction to perform
		self.ops = ops
		self.reads = [t for t in ops if t.type == 'r']
		self.writes = [t for t in ops if t.type == 'w']
		self.AbortNow = False
	def register(self, mgr):
		self.id = mgr.registerTransaction()	
		
class TwoPLTransaction(Transaction):

	def __init__(self, ops):
		super(TwoPLTransaction, self).__init__(ops)
		
	#def register(self, mgr):
	#	self.id = mgr.registerTransaction()
		
		
		
class HekatonTransaction(Transaction):

	def __init__(self, ops):
		self.state = STATE_ACTIVE
		self.CommitDepCounter = 0
		#self.AbortNow = False
		self.CommitDepSet = []
		self.TS = None
		super(HekatonTransaction, self).__init__(ops)
	
	#def register(self, mgr):
	#	self.id = mgr.registerTransaction()
		
	def incrDepCounter(self):
		self.CommitDepCounter += 1
		
	def decrDepCounter(self):
		self.CommitDepCounter -= 1
		
	def registerDep(self, T):
		if self.state == STATE_PREPARING:
			self.CommitDepSet.append(T)
		elif self.state == STATE_ABORTED:
			T.AbortNow = True
		elif self.state == STATE_COMMITTED:
			T.decrDepCounter()
		