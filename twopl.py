import time

class TwoPL:

	def __init__(self, db):
		self.db = db
		return
		
	def initTransaction(self, T):
		numOps = len(T.ops)
		numLocks = 0
		db = self.db
		start = time.time()
		# acquire locks
		while numLocks != numOps:
			# acquire locks
			for op in T.ops:
				if op.type == 'r' and not op.hasLock:
					op.hasLock = db.get(op.key).acquire_RLock()
					if op.hasLock:
						numLocks += 1
				elif op.type == 'w' and not op.hasLock:
					op.hasLock = db.get(op.key).acquire_WLock()
					if op.hasLock:
						numLocks += 1
			# timeout, release locks and start again after delay
			if time.time() - start > 1:
				for op in T.ops:
					if op.type == 'r' and op.hasLock:
						db.get(op.key).release_RLock()
						op.hasLock = False
						numLocks -= 1
					elif op.type == 'w' and op.hasLock:
						db.get(op.key).release_WLock()
						op.hasLock = False
						numLocks -= 1
				time.sleep(0.01)
				start = time.time()
		# perform operations
		for op in T.ops:
			assert(op.hasLock)
			if op.type == 'r':
				op.value = db.get(op.key).value
			elif op.type == 'w':
				db.set(op.key, op.value)
		
		# release locks
		for op in T.ops:
			assert(op.hasLock)
			if op.type == 'r':
				db.get(op.key).release_RLock()
			elif op.type == 'w':
				db.get(op.key).release_WLock()
			
		return T
