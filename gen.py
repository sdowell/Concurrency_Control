import transaction
import random
import pickle
import argparse

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
	



def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("-s", "--size", help="size of db")
	parser.add_argument("-t", "--trans", help="number of transactions")
	parser.add_argument("-c", "--conc", help="concurrency protocol (either 2pl or hek) (default: 2pl)")
	parser.add_argument("-w", "--write", help="ratio of write to read transactions")
	parser.add_argument("-f", "--file", help="name of file to write pickle")
	args = parser.parse_args()
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
	else:
		print("Expected filename")
		return
	
	bmark = []
	perc = int(ratio * 100)
	mgr = transaction.TransactionManager()
	for i in range(num_t):
		if i % 100 >= perc:
			t = genROTrans(size, protocol)
		else:
			t = genRWTrans(size, protocol)
		t.register(mgr)
		bmark.append(t)
	random.shuffle(bmark)
	with open(fname, 'wb') as handle:
		pickle.dump(bmark, handle)
	
	
		

if __name__ == "__main__":

	main()