import hekaton
import hekdb
import queue
import threading
import transaction
import random
import time
import twopl
import argparse
import db
done = False
aborted = 0
total = 0

def genROTrans(size, protocol):
	ops = []
	for i in range(4):
		ops.append(transaction.Operation("r", random.randint(1,size-1)))
	if protocol == "2pl":
		t = transaction.TwoPLTransaction(ops)
	elif protocol == "hek":
		t = transaction.HekatonTransaction(ops)
	return t
	
def genRWTrans(size, protocol):
	ops = []
	for i in range(2):
		ops.append(transaction.Operation("r", random.randint(1,size-1)))
	for i in range(2):
		ops.append(transaction.Operation("w", random.randint(1,size-1), random.randint(0,1000)))
	if protocol == "2pl":
		t = transaction.TwoPLTransaction(ops)
	elif protocol == "hek":
		t = transaction.HekatonTransaction(ops)
	return t
	
def singleTest():
	return
	
def countTrans(T):
	global aborted
	global total
	if T.AbortNow:
		aborted += 1
	total += 1
	return
	
def getDone():
	global done
	return done
	
def producerFunc(send_q, num_trans, size, protocol, ratio):
	#print("Starting producer")
	mgr = transaction.TransactionManager()
	perc = int(ratio * 100) 
	for i in range(num_trans):
		#print("Placing trans on q")
		if i % 100 >= perc:
			t = genROTrans(size, protocol)
		else:
			t = genRWTrans(size, protocol)
		t.register(mgr)
		send_q.put(t)
	#print("Done")
	global done
	done = True
	return
	
def consumerFunc(send_q, server):

	while True:
		try:
			t = send_q.get_nowait()
		except:
			#print("Write queue empty")
			if getDone():
				return
			else:
				continue
		resp = server.initTransaction(t)
		countTrans(resp)
		#receive_q.put(resp)
	
def benchmark(numworkers, size, ntrans, protocol, ratio):
	if protocol == "hek":
		my_db = hekdb.Database(size)
		my_proto = hekaton.Hekaton(my_db)
	else:
		my_db = db.Database(size)
		my_proto = twopl.TwoPL(my_db)
	send_q = queue.Queue()
	num_trans = ntrans
	producer = threading.Thread(target=producerFunc, args = (send_q, num_trans, size, protocol, ratio))
	producer.start()
	consumers = []
	for i in range(0, numworkers):
		consumer = threading.Thread(target=consumerFunc, args = (send_q, my_proto))
		consumers.append(consumer)
		consumer.start()
	producer.join()
	print("producer thread closed")
	for c in consumers:
		c.join()
	print("consumer threads closed")
	print("num_aborted: " + str(aborted))
	print("Total: " + str(total))
	
def main():

	print("Starting client...")
	parser = argparse.ArgumentParser()
	parser.add_argument("-n", "--numworkers", help="number of worker threads (default: 10)")
	parser.add_argument("-s", "--size", help="size of db")
	parser.add_argument("-t", "--trans", help="number of transactions")
	parser.add_argument("-c", "--conc", help="concurrency protocol (either 2pl or hek) (default: 2pl)")
	parser.add_argument("-w", "--write", help="ratio of write to read transactions")
	args = parser.parse_args()
	if args.numworkers:
		numworkers = int(args.numworkers)
	else:
		numworkers = 10
	if args.size:
		size = int(args.size)
	else:
		print("Expected db size")
		return
	if args.trans:
		num_t = int(args.trans)
	else:
		print("Expected trans")
		return
	if args.conc and args.conc == "2pl":
		protocol = args.conc
	elif args.conc and args.conc == "hek":
		protocol = args.conc
	else:
		protocol = "2pl"
	if args.write:
		ratio = float(args.write)
	else:
		ratio = 0	
	
	start = time.time()
	benchmark(numworkers, size, num_t, protocol, ratio)
	end = time.time()
	print("Time elapsed: " + str(end - start))	
	return

if __name__ == "__main__":
	main()