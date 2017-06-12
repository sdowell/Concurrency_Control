import transaction
import sys
import socket
import message
import random
import argparse
import queue
import threading
import time
import pickle
done = False
aborted = 0
total = 0

def requestTransaction(server, T):
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	#print("Attempting to connect to port: " + str(server))
	try:
		s.connect(server)
	except ConnectionRefusedError:
		print("Could not connect to host")
		return None
	t_message = message.TransactionRequest(T)
	s.send(t_message.data)
	response = s.recv(4096)
	#time.sleep(delay)
	success = message.Message.deserialize(response)
	s.close()
	return success

	
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
	perc = int(ratio * 100) 
	for i in range(num_trans):
		#print("Placing trans on q")
		if i % 100 >= perc:
			t = genROTrans(size, protocol)
		else:
			t = genRWTrans(size, protocol)
		send_q.put(t)
	#print("Done")
	global done
	done = True
	return
	
def produceFromFile(send_q, bmark):
	for t in bmark:
		send_q.put(t)
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
		resp = requestTransaction(server, t)
		if resp:
			countTrans(resp.transaction)
		else:
			send_q.put(t)
		#receive_q.put(resp)


		
def benchmark(numworkers, size, ntrans, server, protocol, ratio, bmark):
	send_q = queue.Queue()
	num_trans = ntrans
	
	if bmark is None:
		producer = threading.Thread(target=producerFunc, args = (send_q, num_trans, size, protocol, ratio))
	else:
		producer = threading.Thread(target=produceFromFile, args = (send_q, bmark))
	#producer = threading.Thread(target=producerFunc, args = (send_q, num_trans, size, protocol, ratio))
	producer.start()
	consumers = []

	
	for i in range(0, numworkers):
		consumer = threading.Thread(target=consumerFunc, args = (send_q, server))
		consumers.append(consumer)
		consumer.start()
	producer.join()
	print("producer thread closed")
	for c in consumers:
		c.join()
	print("consumer threads closed")
	print("num_aborted: " + str(aborted))
	print("Total: " + str(total))
	
	return
def main():
	print("Starting client...")
	parser = argparse.ArgumentParser()
	parser.add_argument("-n", "--numworkers", help="number of worker threads (default: 10)")
	parser.add_argument("-p", "--port", help="port number for server")
	parser.add_argument("-s", "--size", help="size of db")
	parser.add_argument("-t", "--trans", help="number of transactions")
	parser.add_argument("-c", "--conc", help="concurrency protocol (either 2pl or hek) (default: 2pl)")
	parser.add_argument("-w", "--write", help="ratio of write to read transactions")
	parser.add_argument("-f", "--file", help="filename for pickle file")
	args = parser.parse_args()
	
	if args.numworkers:
		numworkers = int(args.numworkers)
	else:
		numworkers = 10
	if args.port:
		port = int(args.port)
	else:
		print("Expected port number")
		return
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
	if args.file:
		fname = args.file
		with open(fname, 'rb') as handle:
			bmark = pickle.load(handle)
	else:
		fname = None
		bmark = None		
	server = ('localhost', int(port))
	start = time.time()
	benchmark(numworkers, size, num_t, server, protocol, ratio, bmark)
	end = time.time()
	print("Time elapsed: " + str(end - start))
	return
	
	t1 = transaction.Transaction([transaction.Operation('r',25)])
	r1 = requestTransaction(server, t1)
	print("T1 response: " + str(r1.transaction.ops[0].value))
	val = r1.transaction.ops[0].value
	t2 = transaction.Transaction([transaction.Operation('w',25, val + 1)])
	r2 = requestTransaction(server, t2)
	print("T2 response: " + str(r2.transaction.ops[0].value))
	t3 = transaction.Transaction([transaction.Operation('r',25)])
	r3 = requestTransaction(server, t3)
	print("T3 response: " + str(r3.transaction.ops[0].value))
	return


if __name__ == "__main__":
	main()
