import socket
import sys
import os
import types
import time
import selectors
import threading
import pickle
import time
sel = selectors.DefaultSelector()

HOST = '127.0.0.1'  # server's host address
PORT1 = 42000 # server's port number
PORT2 = 42010 
PORT3 = 42020 
size = 128
PORT = list(42000, 42010, 42020)

'''
Datacenter:
1. store data (key : value)
2. for each client, maintain a dependency list (key, (timestamp, datacenter_id))
3. knows clients' and other datacenters' portnumbers
'''

'''current datacenterID, portnumber, data value and clients' lists stored in datacenter'''
class datacenter:
    def __init__(self, id, datacenter_port, key_value_version):
        self.id = id   # current datacenter ID
        self.datacenter_port = datacenter_port #current datacenter port number
        self.key_vlaue_version = key_value_version # list(list)--(key1,value1,version1), (key2,value2,version2)
        # self.client_lists = client_lists # (???)

class causal_consistency:
    def __init__(self, filename, chunk_num, chunk_hash, client_ip, client_port):
        self.filename = filename
        self.chunk_num = chunk_num
        self.chunk_hash = chunk_hash
        self.client_ip = client_ip
        self.client_port = client_port
        self.datacenter_id = 0
        self.causal_consistency_dict = {}
        self.version_dict = {}
        self.version_dict[self.filename] = 0   # version number of file
    def get_version(self, filename):
        return self.version_dict[filename]

    def update_version(self, filename):
        self.version_dict[filename] += 1
        return self.version_dict[filename]

    def k_v_pair(k, v):
        return k + v
    
    def write(key, value):
        self.causal_consistency_dict[key] = value
        return self.causal_consistency_dict[key]
    
    def read(key):
        return self.causal_consistency_dict[key]
        
    def receive_message(conn, message):
        global lamport_time
        lamport_time = message['time']
        return message

    def send_message(conn, message):
        global lamport_time
        lamport_time += 1
        message['time'] = lamport_time
        conn.send(pickle.dumps(message))
    
    def add_kv_pair(self, k, v):
        return k + v
    
    def get_kv_pair(self, k):
        return k

def Requesthandler(cur_datacenter, conn, addr, client_list):
    print('Enter the handler')
    while True:
        #print('111')
        try:
            data1 = conn.recv(2048)
            request_argu = pickle.loads(data1)
            print(request_argu)

            if request_argu[0] == 'read': # 'read' read_key
                print('Received a read request from client on key =', request_argu[1])
                read_key = request_argu[1]
                if cur_datacenter.key_value_version. == None:  ???
                    conn.sendall(pickle.dumps('There is no such key in this datacenter!'))
                else:
                    key_value = cur_datacenter.keyvalue.get(read_key)   ???
                    conn.sendall(pickle.dumps('Read(key=', read_key, ', value=', key_value))
                    version = list(timestamp, cur_datacenter.id) ?????
                    client_list.append((read_key, version))
                    print('Appended', (read_key, version), 'to this client_list!')

            if request_argu[0] == 'write': # 'write' write_key write_value
                write_key = request_argu[1]
                write_value = request_argu[2]
                print('Received a write request from client on key =', write_key, 'change value to', write_value)
                # update the stored key value
                cur_datacenter.keyvalue[write_key] = write_value
                # propogate the replicated write request to other datacenter
                for i in range(len(PORT)):
                    while i != cur_datacenter.id:
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ss:
                            ss.connect((HOST, PORT[i]))
                            print('Successfully connected to another datacenter', i, '!')
                            time.sleep(10 + i) ???
                            ss.sendall(pickle.dumps(('replicated write request', write_key, write_value, client_list)))
                            print('Sent out the replicated write request!')
                # update current client_list
                client_list = []
                version = list(timestamp, cur_datacenter.id)   ????
                client_list.append((write_key, version))

            if request_argu[0] == 'replicated write request':
                client_list = request_argu[3]
                write_key = request_argu[1]
                write_value = request_argu[2]
                # dependency check
                dependency_check(client_list)
                # if satisfy, commit the write request

                # if not, delay until get satisfied

              
                  
 
        except EOFError:
            print("Client", addr ,"Seems Offline, stop serving it!")
            conn.close()
            return 

def dependency_check(client_list):
    print('Processing dependency check now.')
    print('Recieved client_list is', client_list)

    return 1




'''Main routine and Set up the listening socket'''
if __name__ == "__main__":

    '''Initialize the datacenter'''
    cur_ID = input('Please enter current datacenter ID to initialize:')
    cur_datacenter_port = PORT[cur_ID]
    cur_datacenter = datacenter(cur_ID, cur_datacenter_port, list())
    


    '''create a server socket to listen on'''
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, cur_datacenter_port))
        s.listen()
        ##Used to make a seperate thread for every request
        #def making_thread(s):
        while True:
            conn, addr = s.accept()
            print('Accepted connection from', addr)

            # create a new dependency list for the connected client
            client_list = list()

            threading.Thread(target = Requesthandler, args = (cur_datacenter, conn, addr, client_list)).start()

            
        
            '''
            cur_thread = threading.Thread(target = register_reply, args = (conn, IP_port_filename))
            cur_thread.start()
            '''

              
