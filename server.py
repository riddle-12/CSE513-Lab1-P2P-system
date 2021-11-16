import socket
import sys
import os
import types
import time
import selectors
import threading
import pickle
sel = selectors.DefaultSelector()

HOST = '127.0.0.1'  # server's host address
PORT = 42000 # server's port number
size = 128
#DIR = './'

'''
SERVER: has number of files(16), file names(string),
and a file length(32)
For download: holds nunmber of clients(16),
for each client, chunks of file it has(), client's IP(32) & port(16)
'''

'''File/Client info stored in server'''
class server:
    def __init__(self, client_info, file_length, file_chunk, file_client_chunk):
        self.client_info = client_info # dict(ip : port)
        self.file_length = file_length # dict(filename : filelength)
        self.file_chunk = file_chunk # dict(filename : chunk_num)     (filename : list of chunk_hash_value)
        self.file_client_chunk = file_client_chunk # dict(filename : dict(dict(ip+port_string) : list[chunk]) ) 


def Requesthandler(cur_server, conn, addr):
    #print('Enter the handler')
    while True:
        #print('111')
        try:
            data1 = conn.recv(2048)
            request_argu = pickle.loads(data1)
            #print(request_argu)



            if request_argu[0] == 'register request':
                IP_port_filename = request_argu[1]
                if IP_port_filename != 0:
                    print('Server received the register request from', IP_port_filename[0:2])
                filename = IP_port_filename[2]
                client_IP = IP_port_filename[0]
                client_port = IP_port_filename[1]
                client_infom = {client_IP : client_port}
                cur_server.client_info[client_IP] = client_infom[client_IP]
                conn.send(pickle.dumps('Get registered'))
            
            
            if request_argu == 'asking for a file list':
                conn.send(pickle.dumps(cur_server.file_chunk))
                print('Server sends out the file list')

            if request_argu[0] == 'asking for file:':
                filename = request_argu[1]
                client_hold_chunklist = request_argu[2]
                print('client is asking for the file:', filename)
                print('The file_client_chunk list is:', cur_server.file_client_chunk[filename])
                client_chunk_info = cur_server.file_client_chunk[filename]
                if len(cur_server.file_client_chunk[filename]) == 1: #if only server owns this file
                    print('Only server has this specific file')
                    i = 1
                    while i <= cur_server.file_chunk[filename]:
                        if i not in client_hold_chunklist:
                            f = open('serverchunk'+str(i)+filename, 'r')
                            chunk_data = f.read()
                            num_data = (i, chunk_data)
                            conn.sendall(pickle.dumps(num_data))
                            f.close()
                            print('Send out the chunk_data')
                            break
                        i += 1
                
                else:  #else, use a 'rarest first' selection to decide which peer/which chunk should be connect/downloaded first
                    rarest = selection(cur_server, conn, filename, client_hold_chunklist)
                    i = 0
                    for client in cur_server.file_client_chunk[filename].keys():
                        #print(client)
                        if client != 'SERVER':  #127....
                            chunk_list = cur_server.file_client_chunk[filename][client]
                            if chunk_list.count(rarest) >0:
                                client_infom = client  # 'ip : port'
                                #print(type(client_infom), client_infom)
                                c = client.split()   # ['ip', 'port']
                                #print(type(c), c)   #list
                                #tell the connected client to connect to other peers for downloading
                                print('please ask other peers:', c)
                                c.append(rarest)
                                ip_port_chunknum = c    # ['ip', 'port', chunknum]
                                conn.send(pickle.dumps(('Please connect to other peer:', ip_port_chunknum)))
                                break
                            else:
                                i = i+1
            
                            
                            
                    if i == (len(cur_server.file_client_chunk[filename])-1):
                        print('Only server has this specific chunk')
                        f = open('serverchunk'+str(rarest)+filename, 'r')
                        chunk_data = f.read()
                        num_data = (rarest, chunk_data)
                        conn.sendall(pickle.dumps(num_data))
                        f.close()
                        print('Send out the chunk_data') 

                        

            if request_argu[0] == 'chunk register request':
                filename = request_argu[1]
                chunknum = request_argu[2]
                client_info = str(request_argu[3] + ' ' + request_argu[4])
                #print(cur_server.file_client_chunk[filename])
                if cur_server.file_client_chunk[filename].get(client_info)== None: #it is the first time for the client to register for this file
                    #chunknum_str = str(chunknum)
                    chunknum_l = [chunknum]
                    cur_server.file_client_chunk[filename][client_info] = chunknum_l
                    print('Get chunk register successfully')
                    conn.sendall(pickle.dumps(cur_server.file_client_chunk))
                else:   #the file has registed for the file before, but not this chunk
                    cur_server.file_client_chunk[filename][client_info].append(chunknum)
                    print('Get chunk registered successfully')
                    conn.sendall(pickle.dumps(cur_server.file_client_chunk))  
        except EOFError:
            print("Client", addr ,"Seems Offline, stop serving it!")
            conn.close()
            return 
    #return print('No more requests received from the client. Get disconnected.')

##Selection: decide which chunk should be choosen for downloading, return the rarest chunk number
def selection(cur_server, conn, filename, client_hold_chunklist):
    chunk_count = dict()
    client_chunklist = cur_server.file_client_chunk[filename]
    for chunk_num_list in list(client_chunklist.values()):
        if type(chunk_num_list) is int:
            if chunk_count == {} or chunk_count.get(chunk_num_list) == None:
                chunk_count[chunk_num_list] = 1
            else:
                chunk_count[chunk_num_list] = chunk_count[chunk_num_list] + 1
        else:
            for i in range(0, len(chunk_num_list)):
                if chunk_count == {} or chunk_count.get(chunk_num_list[i]) == None:
                    chunk_count[chunk_num_list[i]] = 1
                else:
                    chunk_count[chunk_num_list[i]] = chunk_count[chunk_num_list[i]] + 1
    for key in chunk_count.keys():
        try:
            if client_hold_chunklist.index(key) >= 0:
                chunk_count[key] = 10000
        except ValueError:
            pass
        rarest = min(chunk_count, key=chunk_count.get)
    print('The rarest chunk number is ', rarest)
    return rarest

## This function is used for reading files in chunks by implemnenting yield
def read_file_by_chunk(filename, chunk_size):
        with open(filename, 'r') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    return
                yield chunk

'''Main routine and Set up the listening socket'''
if __name__ == "__main__":

    '''Initialize the server'''
    cur_server = server(dict(), dict(), dict(), dict())
    filename1 = 'file_1.txt'
    filename2 = 'file_2.txt'
    f1 = open(filename1, 'r')
    f2 = open(filename2, 'r')
    file1_size = os.path.getsize('file_1.txt')
    file2_size = os.path.getsize('file_2.txt')
    cur_server.file_length = {filename1:file1_size, filename2:file2_size}

    #path = 'E:\OneDrive - The Pennsylvania State University\课程\2021fall\513\Xu_Yixin_1/'
    file1_chunk_num = int(file1_size / size)
    if (file1_size % size) != 0:
        file1_chunk_num += 1
    file2_chunk_num = int(file2_size / size)
    if (file2_size % size) != 0:
        file2_chunk_num += 1

    chunks = read_file_by_chunk(filename1, size)
    i1 = 0
    for chunk in chunks:
        i1 += 1
        sf1 = open('serverchunk'+str(i1)+filename1, 'w')
        sf1.write(chunk)
        sf1.close()

    chunks = read_file_by_chunk(filename2, size)
    i2 = 0
    for chunk in chunks:
        i2 += 1
        sf2 = open('serverchunk'+str(i2)+filename2, 'w')
        sf2.write(chunk)
        sf2.close()
    f1.close()
    f2.close()

    cur_server.file_chunk = {filename1:file1_chunk_num, filename2:file2_chunk_num}
    for i in range(0, file1_chunk_num):
        if cur_server.file_client_chunk == {}:
            cur_server.file_client_chunk[filename1] = {'SERVER' : []}
        else:
            if cur_server.file_client_chunk.get(filename1) == None:
                cur_server.file_client_chunk[filename1] = {'SERVER' : []}    
        cur_server.file_client_chunk[filename1]['SERVER'].append(i+1)
        #print(cur_server.file_client_chunk[filename1]['SERVER'])
    for i in range(0, file2_chunk_num):
        if cur_server.file_client_chunk == {}:
            cur_server.file_client_chunk[filename2] = {'SERVER' : []}
        else:
            if cur_server.file_client_chunk.get(filename2) == None:
                cur_server.file_client_chunk[filename2] = {'SERVER' : []}
        cur_server.file_client_chunk[filename2]['SERVER'].append(i+1)
        #print(cur_server.file_client_chunk[filename2]['SERVER'])


    #cur_thread = threading.Lock()
    '''create a server socket to listen on'''
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        ##Used to make a seperate thread for every request
        #def making_thread(s):
        while True:
            conn, addr = s.accept()
            print('Accepted connection from', addr)
            threading.Thread(target = Requesthandler, args = (cur_server, conn, addr)).start()

            
        
            '''
            cur_thread = threading.Thread(target = register_reply, args = (conn, IP_port_filename))
            cur_thread.start()
            '''

              