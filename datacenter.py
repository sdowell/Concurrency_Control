import socket
import threading
import socketserver
import db
import twopl
import sys
import message
import transaction
import time
import argparse
import hekaton
import hekdb
my_db = None
protocol = None
t_mgr = None

class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):
	
	def handle(self):
		message_in = recieve_message(self.request)
		cur_thread = threading.current_thread()
		response_message = handle_message(message_in, self.request)
		send_message(self.request, response_message)

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
	
	def __init__(self, server, handler):
		self.request_queue_size = 100
		#self.server_address = server
		#self.RequestHandlerClass = handler
		super(ThreadedTCPServer, self).__init__(server, handler)
	
	def __exit__(self):
		self.shutdown()

def recieve_message(a_socket):
	m_in = message.Message.deserialize(a_socket.recv(2048))
	#time.sleep(delay)
	#print("Recieved message of type: %s from %s" % (str(type(m_in)), str(a_socket.getpeername())))
	return m_in

def send_message(a_socket, m_out = None):
	if m_out is not None:
		a_socket.send(m_out.serialize())
		#print("Sent message of type: %s to %s" % (str(type(m_out)), str(a_socket.getpeername())))
		
		
def handle_message(our_message, our_socket):
	global protocol
	global t_mgr
	if type(our_message) is message.TransactionRequest:
		our_message.transaction.register(t_mgr)
		response = message.TransactionResponse(protocol.initTransaction(our_message.transaction))
		return response
	else:
		pass
	return

def main():
	global my_db
	global protocol
	global t_mgr
	parser = argparse.ArgumentParser()
	parser.add_argument("-p", "--port", help="port number for server")
	parser.add_argument("-s", "--size", help="size of db")
	parser.add_argument("-c", "--conc", help="concurrency protocol (either 2pl or hek) (default: 2pl)")
	args = parser.parse_args()
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
	if args.conc and args.conc == "2pl":
		my_db = db.Database(size)
		protocol = twopl.TwoPL(my_db)
	elif args.conc and args.conc == "hek":
		my_db = hekdb.Database(size)
		protocol = hekaton.Hekaton(my_db)
	else:
		my_db = db.Database(size)
		protocol = twopl.TwoPL(my_db)
	server_addr = ('localhost', int(port))
	
	t_mgr = transaction.TransactionManager()
	server = ThreadedTCPServer(server_addr, ThreadedTCPRequestHandler)
	#server.request_queue_size = 10
	print(server.request_queue_size)
	server_thread = threading.Thread(target = server.serve_forever)
	server_thread.daemon = True
	server_thread.start()
	print("Starting server on port " + str(port))
	run_server = True
	while run_server:
		try:
			pass
		except KeyboardInterrupt:
			print("Caught keyboard interrupt, shutting down")
			server.__exit__()
			run_server = False		
if __name__ == "__main__":
	main()
