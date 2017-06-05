import socket
import threading
import socketserver
import db
import twopl
import sys
import message
import time

my_db = None
protocol = None

class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):
	
	def handle(self):
		message_in = recieve_message(self.request)
		cur_thread = threading.current_thread()
		response_message = handle_message(message_in, self.request)
		send_message(self.request, response_message)

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
	
	def __exit__(self):
		self.shutdown()

def recieve_message(a_socket):
	m_in = message.Message.deserialize(a_socket.recv(2048))
	#time.sleep(delay)
	print("Recieved message of type: %s from %s" % (str(type(m_in)), str(a_socket.getpeername())))
	return m_in

def send_message(a_socket, m_out = None):
	if m_out is not None:
		a_socket.send(m_out.serialize())
		print("Sent message of type: %s to %s" % (str(type(m_out)), str(a_socket.getpeername())))
		
		
def handle_message(our_message, our_socket):
	global protocol
	if type(our_message) is message.TransactionRequest:
		response = message.TransactionResponse(protocol.initTransaction(our_message.transaction))
		return response
	else:
		pass
	return

if __name__ == "__main__":
	global my_db
	global protocol
	server_addr = ('localhost', int(sys.argv[1]))
	my_db = db.Database(1000)
	protocol = twopl.TwoPL(my_db)
	server = ThreadedTCPServer(server_addr, ThreadedTCPRequestHandler)
	server_thread = threading.Thread(target = server.serve_forever)
	server_thread.daemon = True
	server_thread.start()
	
	run_server = True
	while run_server:
		try:
			pass
		except KeyboardInterrupt:
			print("Caught keyboard interrupt, shutting down")
			server.__exit__()
			run_server = False	
