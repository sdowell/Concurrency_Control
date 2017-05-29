import socket
import threading
import socketserver

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
	m_in = message.Message.deserialize(a_socket.recv(5))
	time.sleep(delay)
	print("Recieved message of type: %s from %s" % (str(type(m_in)), str(a_socket.getpeername())))
	return m_in

def send_message(a_socket, m_out = None):
	if m_out is not None:
		a_socket.send(m_out.serialize())
		print("Sent message of type: %s to %s" % (str(type(m_out)), str(a_socket.getpeername())))
		
		
def handle_message(our_message, our_socket):

	return