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
SERVER_PORT = 42000 # server's port number
size = 128
#client_port = 4500
client_hostname = socket.gethostname() # client hostname
client_IP = socket.gethostbyname(client_hostname) # client IP address
'''
For each client: has their own IP addr(32) and port(16).
For register: the number of files(16), file's name(string)
and file length(32)
'''
client_port = random.randint(20000, 40000) # client port number

class client:
    def __init__(self, IP, port):
        self.ip = IP
        self.port = port

##Register request: tell server this client info
def register_request(s, client_IP, client_port, filename):
    IP_port_filename = (client_IP, client_port, filename)
    #s.sendall(pickle.dumps('register request'))
    s.sendall(pickle.dumps(('register request',IP_port_filename)))
    print('Send register request from', client_IP, ':', client_port, 'to server', 'with file', filename)
    reply = s.recv(2048)
    regi_reply = pickle.loads(reply)
    if regi_reply == 'Get registered':
        print('Successfully get registered from server!')
        return 1
    else:
        return 0

##File list request: ask server for the list of files
def file_list_request(s):
    s.sendall(pickle.dumps('asking for a file list'))
    reply = s.recv(2048)
    file_list = pickle.loads(reply)   # dict(filename : chunknum)
    #print(file_list)
    if file_list:
        print('Successfully get the file list from server!')
        return file_list

##File chunk request: for specific file, ask server for the file data till get the complete file.
def file_chunk_request(s, filename, file_list):
    if file_list.get(filename):
        recv_chunklist = list()
        while len(recv_chunklist) < file_list[filename]: #keep checking if it gets all chunks. If no, keep sending requests.
            s.sendall(pickle.dumps(('asking for file:', filename, recv_chunklist)))
            tmp = s.recv(2048)
            msg = pickle.loads(tmp) #(chunknum, chunkdata)    or 'Please connect to other peer:',  ['ip', 'port', chunknum]
            if msg[0] == 'Please connect to other peer:':
                chunknum = msg[1][2]
                peer_ip = msg[1][0]
                peer_port = int(msg[1][1])
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sc:
                    sc.connect((peer_ip, peer_port))
                    print('Successfully connected to a peer')
                    peer_msg = [filename, chunknum]
                    sc.sendall(pickle.dumps(peer_msg))    # [filename, chunknum]
                    tmp = sc.recv(2048)
                    num_data = pickle.loads(tmp)
                    if num_data[1]:
                        print('Successfully get', filename, 'chunk_NO.', num_data[0], 'from peer', msg[1][0],':', msg[1][1])
                        recv_chunklist.append(num_data[0])
                        f = open(str({client_IP,client_port})+'chunk'+str(num_data[0])+filename, 'w')
                        f.write(num_data[1])
                        command = input('Do you want to share this file chunk? yes or no')
                        if command == 'yes':
                            chunk_register_request(s, filename, num_data[0], str(client_IP), str(client_port))
                    else:
                        print('Failed get the chunk_data')
                        return 0
            else:  # get chunk data from the server
                if msg[1]: #if the chunk data is not empty
                    num_data = msg
                    print('Successfully get', filename, 'chunk_NO.', num_data[0])
                    recv_chunklist.append(num_data[0])
                    f = open(str({client_IP,client_port})+'chunk'+str(num_data[0])+filename, 'w')
                    f.write(num_data[1])
                    command = input('Do you want to share this file chunk? yes or no')
                    if command == 'yes':
                        chunk_register_request(s, filename, num_data[0], str(client_IP), str(client_port))
                else: #if it's empty, fail
                    print('Failed get the chunk_data')
                    return 0
        return print("Get the complete file!")
    else:
        print('No such file in the list！')
        return 0

##Chunk register request: tell server what a new chunk the client has and becomes source
def chunk_register_request(s, filename, chunknum, client_IPstr, client_portstr):
    s.sendall(pickle.dumps(('chunk register request', filename, chunknum, client_IPstr, client_portstr)))
    print('Sent the chunk register request to server')
    tmp = s.recv(2048)
    print('Get chunk register successfully! Current file list is:', pickle.loads(tmp))
    return 1

def Downloadrequesthandler(connc, peer_addr):
    tmp = connc.recv(2048)
    filename_chunknum = pickle.loads(tmp)  # [filename, chunknum]
    f = open(str({client_IP,client_port})+'chunk'+str(filename_chunknum[1])+filename_chunknum[0], 'r')
    chunk_data = f.read()
    num_data = (filename_chunknum[1], chunk_data)        
    connc.sendall(pickle.dumps(num_data))            
    f.close()
    print('Send out the chunk_data')
    #connc.close()
   
    return 1

## create multithreads to handle peer connections
def connect_to_peer():
                
    while True:
        connc, addr = conc.accept()
        threading.Thread(target = Downloadrequesthandler, args = (connc, addr)).start()


'''Main routine and set up socket'''
if __name__ == "__main__":
    cur_client = client(client_IP, client_port)

    #create a socket for communicating with other peers
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as conc:
        conc.bind((client_IP, client_port))
        conc.listen()
        multiprocessing.Process(target = connect_to_peer).start()
    

    ##create a socket for communicating with server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        #s.setblocking(False)
        s.connect((SERVER_HOST, SERVER_PORT))
        print('Successfully connected to server!')
        
        if register_request(s, client_hostname, client_port, '0') == 1:  #register with client_info to server
            while True:
                request_type = input('Do you want to request a file list? yes or no')
                if request_type == 'yes':
                    file_list = file_list_request(s)  #ask for a file list from server
                    print('The whole file list is:', file_list, '(filename : num_chunk)')  #get the whole file list containing (filename : filelength)

                    file_name = input('Please enter the file name you request:') #enter the requested filename
                    file_chunk_request(s, file_name, file_list)
                    

        
              
        
