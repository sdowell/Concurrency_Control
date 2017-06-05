import transaction
import sys
import socket
import message
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
	server = ('localhost', int(sys.argv[1]))
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
