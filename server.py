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
PORT = [62000, 62010, 2020]

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
        self.key_value_version = key_value_version # dict(list)--(key1:(value1,version1), key2:(value2,version2)
        # self.client_lists = client_lists # (???)
lamport_time=0
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
                if cur_datacenter.key_value_version.get(read_key) == None: 
                    conn.sendall(pickle.dumps('There is no such key in this datacenter!'))
                else:
                    LamportClock.receive_message(request_argu[3])
                    key_value = cur_datacenter.key_value_version.get(read_key)[1]   # ???
                    conn.sendall(pickle.dumps('Read(key=', read_key, ', value=', key_value))
                    version = [lamport_time, cur_datacenter.id]
                    client_list.append((read_key, version))
                    print('Appended', (read_key, version), 'to this client_list!')

            if request_argu[0] == 'write': # 'write' write_key write_value
                write_key = request_argu[1]
                write_value = request_argu[2]
                LamportClock.send_message(request_argu[3])
                print('Received a write request from client on key =', write_key, 'change value to', write_value, 'Lamport Clock Value is', lamport_time)
                # update the stored key value
                version = [lamport_time, cur_datacenter.id]
                print('cur_datacenter,id=', cur_datacenter.id)
                print('cur_datacenter.key_value_version = ', cur_datacenter.key_value_version)
                cur_datacenter.key_value_version[write_key] = (write_value, version)
                # propogate the replicated write request to other datacenter
                for i in range(len(PORT)):
                    while i != cur_datacenter.id:
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ss:
                            ss.connect((HOST, PORT[i]))
                            print('Successfully connected to another datacenter', i, '!')
                            time.sleep(1 + i) 
                            ss.sendall(pickle.dumps(('replicated write request', write_key, write_value, client_list)))
                            print('Sent out the replicated write request!')
                # update current client_list
                client_list = []
                client_list.append((write_key, version))

            if request_argu[0] == 'replicated write request':
                client_list = request_argu[3]
                write_key = request_argu[1]
                write_value = request_argu[2]
                # dependency check   # if satisfy, commit the write request  # if not, delay until get satisfied
                while dependency_check(cur_datacenter, client_list) == 0:
                    print('Dependency condition is not satisfied, wait--')
                    time.sleep(1)
                    #buf = dict()
                    #buf[write_key] = list(write_value, client_list[1])
                if dependency_check(cur_datacenter, client_list) == 1:
                    cur_datacenter.key_value_version[write_key] = [write_value, client_list[1]] 
              
                  
 
        except EOFError:
            print("Client", addr ,"Seems Offline, stop serving it!")
            conn.close()
            return 

def dependency_check(cur_datacenter, client_list):
    print('Processing dependency check now.')
    print('Recieved client_list is', client_list)
    # check if receive the version in client_list
    if cur_datacenter.key_value_version.get(client_list[0])[1] == client_list[1]:
        return 1
    else:
        return 0




'''Main routine and Set up the listening socket'''
if __name__ == "__main__":

    '''Initialize the datacenter'''
    cur_ID = int(input('Please enter current datacenter ID to initialize:'))
    cur_datacenter_port = PORT[cur_ID]
    tmp = {'x': [0, [0, cur_ID]], 'y': [0, [0, cur_ID]], 'z': [0, [0, cur_ID]]}
    cur_datacenter = datacenter(cur_ID, cur_datacenter_port, tmp)
    print('cur_datacenter.key_value_version = ', cur_datacenter.key_value_version)

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

              
