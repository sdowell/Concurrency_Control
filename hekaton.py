import hekdb
import transaction

class Hekaton:

	def __init__(self, db):
		self.db = db
		self.clock = hekdb.LogicalClock()
		return
		
		
		
	def initTransaction(self, T):
		
		
		begin = self.clock.getTime()
		T.state = transaction.STATE_ACTIVE
		abort = False
		rlockset = []
		wlockset = []
		
		# normal processing
		for op in T.ops:
			if op.type == 'r':
				val = self.db.get(op.key, begin.time, T, rlockset)
				if val == None:
					T.AbortNow = True
					abort = True
					break
				else:
					op.value = val
			elif op.type == 'w':
				success = self.db.set(op.key, op.value, T, wlockset)
				if not success:
					#print("couldn't write")
					T.AbortNow = True
					abort = True
					break
				
			else:
				assert(False)
		end = self.clock.getTime()
		# preparation
		T.TS = end
		T.state = transaction.STATE_PREPARING
		while T.CommitDepCounter > 0 and not T.AbortNow:
			pass
		
		# postprocessing
		if T.AbortNow:
			T.state = transaction.STATE_ABORTED
		else:
			T.state = transaction.STATE_COMMITTED
		
		if T.AbortNow:
			self.db.abort(T, [x.key for x in T.writes], rlockset, wlockset)
		else:
			self.db.commit(T, [x.key for x in T.writes], end.time, rlockset, wlockset)
		
		return T
		
def test():
	mydb = hekdb.Database(1000)
	protocol = Hekaton(mydb)
	tmgr = transaction.TransactionManager()
	
		
if __name__ == "__main__":
	test()