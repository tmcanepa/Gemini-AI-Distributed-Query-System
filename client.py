import google.generativeai as genai
import os
from dotenv import load_dotenv
import socket
import threading
import time
import sys
import heapq


load_dotenv()

gemini_api = os.getenv("API_KEY")

genai.configure(api_key=gemini_api)
model = genai.GenerativeModel("gemini-1.5-flash")
# response = model.generate_content("Explain how AI works")
# print(response.text)





my_socket= None
client_sockets = []
server_sockets =[]
running = True
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


def handle_exit():
    for client in client_sockets:
        client.close()
    for server in server_sockets:
        server.close()
    if my_socket:
        my_socket.close()
    server_sockets.clear()  
    client_sockets.clear()  
    sys.stdout.flush()
    sys.exit(0)


def talk_to_server(message, client_id):
    global active_peer_threads
    global server_socket
    if message.lower() == "exit":
        handle_exit()
    else:
        server_socket.send(message.encode('utf-8'))
        response = server_socket.recv(1024).decode('utf-8')
        print(f"received {response}")
        #handle_function() ie create,query, 
    

def start_client(): #Allows other clients to talk to you, also connect to other clients from peer list
    global server_socket
    server_socket.connect(('127.0.0.1', port))
    while running:
            message = input()
            threading.Thread(target=talk_to_server, args=(message, client_id), daemon=True).start()


    

if __name__ == "__main__":
    global port
    global client_id
    if len(sys.argv) < 3:
        sys.exit(1)
    port = int(sys.argv[2])
    client_id = int(sys.argv[1])
    start_client()