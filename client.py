import chunk
from concurrent.futures import thread
from email import message
from http import client
from pydoc import cli
import socket 
import threading
from time import sleep


counter = 0 
lock = threading.Lock()
PORT_SERVER_UDP = 5504

PORT_SERVER_TCP = 5055

server = '0.0.0.0'

clients = [] 

clients_chunks = [[] , [] , [] , [] , []]     # 5 elements ... each element of the form of list of (chunk_id , chunk_size , msg )

map_chunk_id_to_ind = [{} , {} , {} , {} , {}]         #dictionary mapping the chunk_id to index at which the chunk is present  for each client 

CHUNK_ID_SIZE = 8 

CHUNK_SIZE_SIZE = 4 

HEADER_SIZE = CHUNK_ID_SIZE+CHUNK_SIZE_SIZE

CHUNK_SIZE = 1024




TOTAL_SIZE = HEADER_SIZE + CHUNK_SIZE

def new_client(client_port_udp):
    global PORT_SERVER_UDP, PORT_SERVER_TCP, server

    server_ADDR_TCP = (server, PORT_SERVER_TCP )

    #tcp socket init
    
    client_TCP = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_TCP.connect(server_ADDR_TCP)
    print(f'The client port#{client_TCP.getsockname()[1]} is connected(tcp)')

    #udp socket init

    client_UDP = socket.socket(socket.AF_INET , socket.SOCK_DGRAM)
    client_UDP.bind((server , client_port_udp))
    client_cnct_msg_udp = 'HELLO!'  #sending hi to server to save addr
    client_cnct_msg_udp = client_cnct_msg_udp.encode('utf-16')
    client_UDP.sendto(client_cnct_msg_udp,(server,PORT_SERVER_UDP))
    print(f'The client port#{client_UDP.getsockname()[1]} is connected(udp)')
    
    clients.append((client_TCP, client_UDP.getsockname()))




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

    print('connecting 5 clients to server ...')
  
   
    for client_num in range(0 , 5):
        new_client(client_num+6000)
  

init_clients()
# acquire_file_chunks(clients=clients)

def receive_chunk(client, id ):
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
            data = msg[40:]
            data= data.decode('utf-16')
            print(f'chunk {chunk_id} received by client {client.getsockname()}')
            clients_chunks[id].append((chunk_id , chunk_size , msg))
            local_chunks.append((chunk_id , chunk_size , msg))
            local_map[chunk_id] = len(local_chunks) - 1 

    #   requesting the chunks that are not recieved by the client 

    
            
        
threads = [] 

def acquire_file_chunks(clients):
 

    for i in range (0  , 5 ):
        thread = threading.Thread(target=receive_chunk, args=(clients[i][0], i ) )
        threads.append(thread)
        thread.start()
    for x in threads:
        x.join()



acquire_file_chunks(clients)





