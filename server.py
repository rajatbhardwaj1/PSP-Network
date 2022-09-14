from base64 import encode
import chunk
from concurrent.futures import thread
from glob import glob
from http import client
import imp
from lzma import CHECK_NONE
import time 

import socket
from sqlite3 import Connection 
import threading
from xmlrpc.client import Server

PORT_SERVER_UDP = 5504

PORT_SERVER_TCP = 5055


# HEADERS SIZES 

CHUNK_ID_SIZE = 8 

CHUNK_SIZE_SIZE = 4 

HEADER_SIZE = CHUNK_ID_SIZE+CHUNK_SIZE_SIZE

CHUNK_SIZE = 1024

client_port_id = []   #will be a tuple of (conn_tcp , addr_udp)



SERVER = '0.0.0.0'

ADDR_SERVER_UDP = (SERVER , PORT_SERVER_UDP)
ADDR_SERVER_TCP = (SERVER , PORT_SERVER_TCP)



DISCONNECT_MSG = "!DISCONNECT"


server_UDP = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
server_UDP.bind(ADDR_SERVER_UDP)



server_TCP = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_TCP.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_TCP.bind(ADDR_SERVER_TCP)
server_TCP.listen()






def new_connection():                           #not implemented for udp
    # conn_UDP , addr_UDP = server_UDP.accept()
  
    while True :
        conn_TCP , addr_TCP = server_TCP.accept()
       

        print(f'TCP connection is active with client [{addr_TCP}]')

        client_port_UDP, addr_UDP = server_UDP.recvfrom(100)
      
        

        print(f'UDP connection is active with client [{addr_UDP}] ')

        print(f'appending udp and tcp ports to list ....')

        client_port_id.append((conn_TCP, addr_UDP))
        if len(client_port_id) == 5:
            break
     
        

def make_header(chunk_id ,  chunk_size ):
    global CHUNK_ID_SIZE , CHUNK_SIZE_SIZE 
    p1 = str(chunk_id).encode()
    p1 += b' '*(CHUNK_ID_SIZE - len(p1))

    p2 = str(chunk_size).encode()
    p2 += b' '*(CHUNK_SIZE_SIZE - len(p2))

    header = p1 + p2 
    return header 

    

def adjust_data_size(data):
    data += b' '*(CHUNK_SIZE - len(data))
    return data


#encoding the file and making 1kb chunks  
def splitfile(filename):
 
    with open(filename,"r") as f:
        text = f.readlines()[0]
        text = text.encode('utf-16')
        length = len(text)
        chunk_list = []
        num_chunks = length // CHUNK_SIZE 
        for chunk_id in range(0 , num_chunks):
            header = make_header(chunk_id , CHUNK_SIZE )
      
            data = text[chunk_id*CHUNK_SIZE: (chunk_id+1)*CHUNK_SIZE]
            chunk_list.append((chunk_id, header + data ))
        if(num_chunks*CHUNK_SIZE < length):
            header = make_header(num_chunks , length - num_chunks*CHUNK_SIZE)

            data = adjust_data_size( text[num_chunks*CHUNK_SIZE: length] )
          
            chunk_list.append((num_chunks, header + data ))
        
        for i in range(5):
            data = '#'.encode() 
            data += str(len(chunk_list)).encode()
            data += b' '*(HEADER_SIZE - len(data))

         
            chunk_list.append(('#', data))

        return chunk_list

def send_chunk(chunk , conn):
    print(f'sending chunk {chunk[0]} to client {conn.getpeername()} ')
    conn.send(chunk[1])


def distribute_file_to_clients(filename):
    chunk_list = splitfile(filename)
    print(len(chunk_list))

    
    for i in range(len(chunk_list)):
        send_chunk(chunk_list[i], client_port_id[i%5][0])



new_connection()
distribute_file_to_clients('OneDrive_1_7-9-2022/A2_large_file.txt')
