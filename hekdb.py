import random
import threading
import transaction

TIME_INFINITY = -1

class LogicalClock:
	def __init__(self):
		self.lock = threading.Lock()
		self.time = 1

	def getTime(self):
		self.lock.acquire()
		tval = self.time
		self.time += 1
		self.lock.release()
		return LogicalTime(tval)
		
class LogicalTime:
	def __init__(self, time):
		self.time = time
		
class RecordLock:

	def __init__(self):
		self.lock = threading.Lock()
		self.num_readers = 0
		self.num_writers = 0
		self.writer = None

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
	
	def acquire_WLock(self, id):
		self.lock.acquire()
		if self.num_readers == 0 and self.num_writers == 0:
			self.num_writers += 1
			self.writer = id
			self.lock.release()
			return True
		else:
			#print("couldn't acquire wlock")
			self.lock.release()
			return False
		
	def release_WLock(self):
		self.lock.acquire()
		assert(self.num_writers > 0)
		self.num_writers = 0
		self.writer = None
		self.lock.release()
		return True

class Record:

	def __init__(self, value, begin, end):
		self.value = value
		self.begin = begin
		self.end = end

		
class Database:

	def __init__(self, size):
		self.store = {}
		self.size = size
		for i in range(size):
			self.store[i] = [Record(random.randint(0, 100), LogicalTime(0), RecordLock())]
			
	def get(self, key, time, T, rlockset):
		if key < 0 or key >= self.size:
			return None
		for r in self.store[key]:
			begin = r.begin
			end = r.end
			value = r.value
			#ts = r.TS
			if begin is None:
				continue
			if type(begin) is LogicalTime and type(end) is LogicalTime:
				if begin.time <= time and (end.time > time or end.time == TIME_INFINITY):
					return value
			elif type(begin) is LogicalTime and type(end) is RecordLock:
				if begin.time <= time:
					return value
					hasLock = end.acquire_RLock()
					if hasLock:
						rlockset.append(end)
						return value
					else:
						return None
			elif type(begin) is transaction.HekatonTransaction:
				if begin.state == transaction.STATE_ACTIVE:
					if begin.id == T.id:
						return value
					else:
						return None
				elif begin.state == transaction.STATE_PREPARING:
					if begin.TS.time < time:
						#speculatively read
						T.CommitDepCounter += 1
						begin.registerDep(T)
						return value
					else:
						return None
				elif begin.state == transaction.STATE_COMMITTED:
					if begin.TS.time <= time:
						return value
					else:
						continue
				elif begin.state == transaction.STATE_ABORTED:
					continue
		
			elif type(end) is transaction.HekatonTransaction:
				if end.state == transaction.STATE_ACTIVE:
					if end.id == T.id:
						return value
					else:
						return None
				elif end.state == transaction.STATE_PREPARING:
					if time < end.TS.time:
						# visible
						return value
					else:
						# speculatively ignore
						T.CommitDepCounter += 1
						r.registerDep(T)
						continue
				elif end.state == transaction.STATE_COMMITTED:
					if time < end.TS.time:
						return value
					else:
						continue
				elif end.state == transaction.STATE_ABORTED:
					continue
		return None
		
	def set(self, key, value, T, wlockset):
		if key < 0 or key >= self.size:
			return False
		
		v = None
		# find most up to date version
		for r in reversed(self.store[key]):
			end = r.end
			if type(end) is RecordLock:
				haslock = end.acquire_WLock(T.id)
				if haslock:
					wlockset.append(end)
					v = r
					break
				else:
					#print("Couldn't acquire wlock")
					return False
			continue
			if type(end) is LogicalTime and end.time == TIME_INFINITY:
				v = r
				break
			elif type(end) is transaction.HekatonTransaction and end.state == transaction.STATE_ABORTED:
				v = r
				break
		
		if v is None:
			#print("V is none")
			return False
		
		new_record = Record(value, T, LogicalTime(TIME_INFINITY))
		#new_record.end.acquire_WLock()
		#wlockset.append(new_record.end)
		self.store[key].append(new_record)
		return True
		
	def commit(self, T, keys, time, rlockset, wlockset):
		for dep in T.CommitDepSet:
			dep.CommitDepCounter -= 1
		#print(type(time))
		for key in keys:
			#print(self.store[key])
			for r in self.store[key]:
				#print(r.begin)
				#print(r.end)
				if type(r.begin) is transaction.HekatonTransaction and r.begin.id == T.id:
					r.begin = LogicalTime(time)
					r.end = RecordLock()
					#assert(type(r.end) is RecordLock)
					#assert(r.end in wlockset)
					#r.end.release_WLock()
				elif type(r.end) is RecordLock:
					#print(r.begin)
					#print(r.end)
					#print(r.value)
					#assert(r.end.writer.value == T.id.value)
					r.end.release_WLock()
					r.end = LogicalTime(time)
				#print("written")
				#print(r.begin)
				#print(r.end)
		for r in rlockset:
			r.release_RLock()
					
		return
		
	def abort(self, T, keys, rlockset, wlockset):
		for dep in T.CommitDepSet:
			dep.AbortNow = True
			dep.CommitDepCounter -= 1
		for key in keys:
			for r in self.store[key]:
				if type(r.begin) is transaction.HekatonTransaction and r.begin.id == T.id:
					r.begin = None
					r.end = None
		for r in rlockset:
			r.release_RLock()
		for w in wlockset:
			w.release_WLock()
		return