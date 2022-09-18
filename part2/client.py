import chunk
from concurrent.futures import thread
from email import message
from glob import glob
import hashlib
from http import client
from multiprocessing import Lock
from pydoc import cli
import socket 
import threading
import time 
from time import sleep
from urllib import request
import logging 
logging.basicConfig(filename="std.log", 
					format='%(asctime)s %(message)s', 
					filemode='w') 
logger=logging.getLogger() 
logger.setLevel(logging.DEBUG) 

counter = 0 
lock = threading.Lock()
PORT_SERVER_UDP = 5504

PORT_SERVER_TCP = 5055

HASHFUNC = hashlib.md5()

start_time = time.time()

ENCODING = 'utf-8'

server = '0.0.0.0'

clients = [] 

clients_chunks = []     #  elements ... each element of the form of list of (chunk_id , chunk_size , msg )

server_broadcast_sock = [] 

map_chunk_id_to_ind = []         #dictionary mapping the chunk_id to index at which the chunk is present  for each client 

CHUNK_ID_SIZE = 8 

CHUNK_SIZE_SIZE = 4 

HEADER_SIZE = CHUNK_ID_SIZE+CHUNK_SIZE_SIZE

CHUNK_SIZE = 1024

TOTAL_SIZE = CHUNK_SIZE + HEADER_SIZE

REQUEST_SIZE = 8 

CLIENT_NAME_SIZE = 8 

REQUEST_SIZE_1 = CLIENT_NAME_SIZE + REQUEST_SIZE


NUM_CLIENTS = 5 

client_broadcast_TCP = [] 


client_binded_tcp = [] 
all_chunks_rec = 0 

for i in range (NUM_CLIENTS):
    clients_chunks.append([])
    map_chunk_id_to_ind.append({})
    




def new_client(client_port_udp):
    global PORT_SERVER_UDP, PORT_SERVER_TCP, server

    server_ADDR_TCP = (server, PORT_SERVER_TCP )

    #tcp socket init
    
    client_TCP = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_TCP.connect(server_ADDR_TCP)


    print(f'The client port #{client_TCP.getsockname()[1]} is connected(tcp)')


    ADDR_SERVER_TCP =   (server , client_port_udp - 2000) #.... 4000,4001 ,....  
    client_TCP_binded = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_TCP_binded.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    client_TCP_binded.settimeout(2)
    client_TCP_binded.bind(ADDR_SERVER_TCP)
    client_TCP_binded.listen()
    client_binded_tcp.append(client_TCP_binded)
    #udp socket init

    client_UDP = socket.socket(socket.AF_INET , socket.SOCK_DGRAM)
    client_UDP.bind((server , client_port_udp))
    client_cnct_msg_udp = 'HELLO!'  #sending hi to server to save addr
    client_cnct_msg_udp = client_cnct_msg_udp.encode(ENCODING)
    client_UDP.sendto(client_cnct_msg_udp,(server,PORT_SERVER_UDP))
    print(f'The client port #{client_UDP.getsockname()[1]} is connected(udp)')
    clients.append((client_TCP, client_UDP))

def make_header(chunk_id ,  chunk_size ):
    global CHUNK_ID_SIZE , CHUNK_SIZE_SIZE 
    p1 = str(chunk_id).encode()
    p1 += b' '*(CHUNK_ID_SIZE - len(p1))

    p2 = str(chunk_size).encode()
    p2 += b' '*(CHUNK_SIZE_SIZE - len(p2))

    header = p1 + p2 
    return header 

    
def decode_headers(headers):       #return chunk_id , chunk_size
    if(headers[0] == '#'):
        chunk_list_size = int(headers[1:HEADER_SIZE])
        return chunk_list_size
    chunk_id = int(headers[0:CHUNK_ID_SIZE])
    chunk_size = int(headers[CHUNK_ID_SIZE : HEADER_SIZE])
    
    return chunk_id , chunk_size


#for initialising the clients 
def init_clients():
    global PORT_SERVER_TCP, PORT_SERVER_UDP, server,lock

    print('connecting clients to server ...')
  
   
    for client_num in range(0 , NUM_CLIENTS):
        new_client(client_num+6000)


def setup_broadcast():
    while True :
        server_port_UDP, addr_UDP = clients[0][1].recvfrom(6)
        server_broadcast_sock.append(addr_UDP)
        print(f'UDP broadcast is active with server #{addr_UDP[1] - 7000} ')
      
        if len(server_broadcast_sock) == NUM_CLIENTS:
            print('Broadcast initialised!')
            break





# print(clients)
# acquire_file_chunks(clients=clients)


def receive_chunk(client, id ):

    global lock , clients_chunks, map_chunk_id_to_ind, HEADER_SIZE , TOTAL_SIZE
    
    connected =  True 
    local_chunks = []       #chunks that the client has received 
    local_map = {}          #dictonary mapping the chunk_id to the index of the chunk in the client 
    server_chunk_list_size = 0 
    while connected:    #   receiving initially from the server 
        msg =client.recv(TOTAL_SIZE)

        header = msg[0:HEADER_SIZE]
        header = header.decode()
        
        if header[0] == '#' :
            chunk_list_size = decode_headers(header)
            clients_chunks[id].append((chunk_list_size , chunk_list_size , chunk_list_size)) #last chunk recieved gives the info about the numbeer of chunks the server had orignaly
            server_chunk_list_size = chunk_list_size 
            connected = False
            break
        else :
            try:
                chunk_id , chunk_size = decode_headers(header)
            except ValueError as v :
                print('value of headers = ', header)
            print(f'client #{id} recieved chunk #{chunk_id} from server')
            data = msg[HEADER_SIZE:]
            
            logger.info(f'chunk {chunk_id} received by client {client.getsockname()}')
        
            
            clients_chunks[id].append((chunk_id , chunk_size , data))
            local_chunks.append((chunk_id , chunk_size , data))
            local_map[chunk_id] = len(local_chunks) - 1 
            map_chunk_id_to_ind[id][chunk_id] = len(local_chunks) - 1 
    client.close()



    
            
        
threads = [] 

def acquire_file_chunks(clients):
    for i in range (0  , NUM_CLIENTS ):
        thread = threading.Thread(target=receive_chunk, args=(clients[i][0], i ) )
        threads.append(thread)
        thread.start()
    for x in threads:
        x.join()
 


def request_chunk(chunk_id  , id ):
    global clients_chunks , map_chunk_id_to_ind 
    logger.info(f'client #{id} requesting chunk id #{chunk_id} from the server!')



    request = str(chunk_id).encode('utf-8')

    request += b' '*(REQUEST_SIZE - len(request))
    while True: 

        
        #making a random udp port 

        client_UDP_random = socket.socket(socket.AF_INET , socket.SOCK_DGRAM)

        client_UDP_random.sendto(request,server_broadcast_sock[id])

        client_UDP_random.close()


        logger.info(f'Client #{id} is ready to acquire missing chunk #{chunk_id}')

        
        #code this ...................................
        cont = False 
        if chunk_id != -1 :
            try :
                conn , addr_serv = client_binded_tcp[id].accept()
                message = conn.recv(TOTAL_SIZE)
                cont = True 

            except  socket.timeout:
                logger.info(f'UDP packet was dropped ... re-requesting the packet #{chunk_id} for client #{id}')
                pass
            if cont == True :
                logger.info(f'client #{id} has recieved the requested chunk #{chunk_id}') 
                header = message[0:HEADER_SIZE]
                header = header.decode()
                chunk_id_1 , chunk_size = decode_headers(header)
                data = message[HEADER_SIZE:]
                logger.info(f'The requested chunk #{chunk_id} received by client #{clients[id][1].getsockname()[1] - 6000} !')
                clients_chunks[id].append((chunk_id , chunk_size , data))
                map_chunk_id_to_ind[id][chunk_id] = len(clients_chunks[id]) - 1 
                break
        else :
            break

    if chunk_id % 1000 == 0 :
        logger.info(f'Time taken for {chunk_id} chunks to be recieved by client #{id} is {time.time() - start_time}')




def request_remaining_files(id):
    

    global lock , clients_chunks, map_chunk_id_to_ind, HEADER_SIZE , TOTAL_SIZE
          
    for chunk_id in range(clients_chunks[id][-1][0]):
        if chunk_id  in map_chunk_id_to_ind[id]  :
            pass
        else:
            request_chunk(chunk_id , id)
            

    server_ADDR_TCP_random = (server, 9000+id)
    client_TCP_random = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_TCP_random.connect(server_ADDR_TCP_random)


    chunk_id , chunk_size , data = -1 , 1 , b' '
    data = adjust_data_size(data)
    
    header = make_header(chunk_id , chunk_size)
    
    sending = header + data 
    client_TCP_random.send(sending)
    
    client_TCP_random.close()
    request_chunk(-1  , id )     
            
def adjust_data_size(data):
    data += b' '*(CHUNK_SIZE - len(data))
    return data

def handle_broadcast(id):           #recieves request from server 
    
    while True :

        request, addr_UDP = clients[id][1].recvfrom(REQUEST_SIZE_1)        
        req_chunk_id = int(request[0:REQUEST_SIZE].decode())

        req_client_id = int(request[REQUEST_SIZE:].decode())
        
        print(f'chunk #{req_chunk_id} request from server to client {id} ')
        if req_chunk_id == -1 :
            break
        #checking if the packet exist 
        
        if req_chunk_id in map_chunk_id_to_ind[id]:
            server_ADDR_TCP_random = (server, 9000+id)
            client_TCP_random = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_TCP_random.connect(server_ADDR_TCP_random)
            chunk_id , chunk_size , data = clients_chunks[id][map_chunk_id_to_ind[id][req_chunk_id]]

            header = make_header(chunk_id , chunk_size)
            
            sending = header + data 
            client_TCP_random.send(sending)
            
            client_TCP_random.close()
            print(f'client #{id} sent requested chunk #{req_chunk_id} to the server and connection is closed!')

            print(f'Client #{id} sending chunk #{chunk_id} to the server port #{req_chunk_id}')
        else :
            pass
    
RRA_threads = [] 
def req_rem_all_c():
    for id in range(NUM_CLIENTS):
        thread = threading.Thread(target=request_remaining_files, args=(id,))
        thread.start()
        RRA_threads.append(thread)
    

SRT_threads = [] 
def server_request_handel():                #broadcast handler , sending chunks parallely to the server 
    for id in range(NUM_CLIENTS):
        thread = threading.Thread(target=handle_broadcast, args=(id,))
        thread.start()
        SRT_threads.append(thread)

def combine(id ):
    i = len(clients_chunks[id])
    f = open ('client_'+str(id)+'.txt' , 'w')
    w = open ('testing.txt' , 'w')
    
    array = bytearray(b'')
    logger.info(f'The last chunk is {len(map_chunk_id_to_ind[id])}')
    for chunk_id in range(len(clients_chunks[id]) - 1):
        chunk_id_1 , chunk_size , data  = clients_chunks[id][map_chunk_id_to_ind[id][chunk_id]]
        data =  data[0:chunk_size]

        for b in data :
            
            array.append(b)
    
    bytes_obj = bytes(array)
    w.write(str(bytes_obj))
    bytes_obj = bytes_obj.decode(ENCODING)
    f.write(bytes_obj)
    f.close()



        


def combine_chunks():
    for id in range(NUM_CLIENTS):
        combine(id)


init_clients()
acquire_file_chunks(clients)


setup_broadcast()
req_rem_all_c()

server_request_handel()

for x in RRA_threads:
    x.join()

for x in SRT_threads:
    x.join()


combine_chunks()


for  i in range(NUM_CLIENTS):
    md5_hash = hashlib.md5()
    md5_hash.update( open("client_"+str(i)+".txt", 'rb').read())
    hash = md5_hash.hexdigest()
    print("md5 sum:",hash)

end_time = time.time() 





logger.info(f'Total time taken by the code = {end_time - start_time}')








