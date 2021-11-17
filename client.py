import socket
import os
import sys
import random
import selectors
import pickle
import time
import threading
import multiprocessing

SERVER_HOST = '127.0.0.1'  # server's host address
SERVER_PORT1 = 42000 # server's port number
SERVER_PORT2 = 42010 
SERVER_PORT3 = 42020
SERVER_PORT = list(42000, 42010, 42020) 

#client_port = 43000
client_hostname = socket.gethostname() # client hostname
client_IP = socket.gethostbyname(client_hostname) # client IP address
client_port = random.randint(20000, 40000) # client port number
'''
For each client: has their own IP addr(32) and port(16).
'''
class client:
    def __init__(self, IP, port):
        self.ip = IP
        self.port = port



'''Main routine and set up socket'''
if __name__ == "__main__":
    cur_client = client(client_IP, client_port)
    
    datacenter_id = input("Please enter the ID of dataserver you want to connect(0/1/2):")
    server_port = SERVER_PORT[datacenter_id]

    ##create a socket for communicating with server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        #s.setblocking(False)
        s.connect((SERVER_HOST, server_port))
        print('Successfully connected to datacenter', datacenter_id, '!')

        while True:
            operation = input('Enter your operation request [write, key, value / read, key]:')
            if operation == 'read':
                read_key = input('Which key do you want to read?')
                s.sendall(pickle.dumps((operation,read_key)))
                msg1 = s.recv(2048)
                print(pickle.loads(msg1))   # get return message from datacenter
            if operation == 'write':
                write_key = input('Which key:')
                write_value = input('Which value:')
                s.sendall(pickle.dumps((operation,write_key,write_value)))
        
