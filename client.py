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
SERVER_PORT = [62000, 62010, 62020] 

#client_port = 43000
client_hostname = socket.gethostname() # client hostname
client_IP = socket.gethostbyname(client_hostname) # client IP address
client_port = random.randint(20000, 40000) # client port number
'''
For each client: has their own IP addr(32) and port(16).
'''
lamport_time = 0


class client:
    def __init__(self, IP, port):
        self.ip = IP
        self.port = port

class LamportClock:
    def __init__(self):
        self.time = 0 

    def receive_message(message):
        global lamport_time
        recv_time = message['time']
        if recv_time > lamport_time:
            lamport_time = recv_time
        return message

    def send_message(message):
        global lamport_time
        print('Current time:', lamport_time)
        lamport_time += 1
        print('Time is now:', lamport_time)
        message['time'] = lamport_time
        return message
    
    def update_time(time):
        global lamport_time
        print('server time is:', time)
        if time>lamport_time:
            lamport_time = time
            

'''Main routine and set up socket'''
if __name__ == "__main__":
    cur_client = client(client_IP, client_port)
    
    datacenter_id = int(input("Please enter the ID of dataserver you want to connect(0/1/2):"))
    server_port = SERVER_PORT[datacenter_id]

    ##create a socket for communicating with server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        #s.setblocking(False)
        s.connect((SERVER_HOST, server_port))
        print('Successfully connected to datacenter', datacenter_id, '!')

        while True:
            operation = input('Enter your operation request [write / read]:')
            if operation == 'read':
                print(client_port)
                read_key = input('Which key do you want to read?')
                lamport = {}
                lamport = LamportClock.send_message(lamport)
                s.sendall(pickle.dumps((operation,read_key,lamport)))
                msg1 = s.recv(2048)
                recv = pickle.loads(msg1)
                LamportClock.update_time(recv[2])
                print('Read out', recv[0], '=',recv[1])   # pickle.dump[a,b] get return message from datacenter

            if operation == 'write':
                write_key = input('Which key:')
                write_value = input('Which value:')
                print(client_port)
                lamport = {}
                lamport = LamportClock.send_message(lamport)
                s.sendall(pickle.dumps((operation,write_key,write_value,lamport)))
        
