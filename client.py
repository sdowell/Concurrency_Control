import TwoPL
import sys

def requestTransaction(server, T):
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	print("Attempting to connect to port: " + str(server))
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


def main():
	print("Starting client...")
	server = ('localhost', sys.argv[1])
	t1 = TwoPL.Transaction([TwoPL.Operation('r',25)])
	r1 = requestTransaction(server, t1)
	print("T1 response: " + r1.T.ops[0].value)
	val = r1.ops[0].value
	t2 = TwoPL.Transaction([TwoPL.Operation('w',25, val + 1)])
	r2 = requestTransaction(server, t2)
	print("T2 response: " + r2.T.ops[0].value)
	t3 = TwoPL.Transaction([TwoPL.Operation('r',25)])
	r3 = requestTransaction(server, t3)
	print("T3 response: " + r3.T.ops[0].value)
	return


if __name__ == "__main__":
	main()
