import google.generativeai as genai
import os
from dotenv import load_dotenv
import socket
import threading
import time
import sys
import heapq
import queue
import ast


load_dotenv()

gemini_api = os.getenv("API_KEY")

genai.configure(api_key=gemini_api)
model = genai.GenerativeModel("gemini-1.5-flash")
# response = model.generate_content("Explain how AI works")
# print(response.text)




key_value_store = []
leader_queue = queue.Queue()
query_queue = queue.Queue()
client_sockets = []
running = True
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_id = 0
curr_leader = 0
ballot_num = [0,0]
# accept_num = [0,0]
# accept_val = None



def handle_exit():
    for client in client_sockets:
        client.close()
    client_sockets.clear()  
    sys.stdout.flush()
    sys.exit(0)

    

def start_client(): #Connect to the coordinator and give it your id and port number 
    global client_socket
    global client_id
    client_socket.connect(('127.0.0.1', port))
    port_num = client_socket.getsockname()[1]
    client_socket.send(f"{client_id} {port_num}".encode())
    response = client_socket.recv(1024).decode()
    if response != "Success":
        print("Failure to send server clientid")
    threading.Thread(target = handle_messages, ).start() #Needed for receiving prepare messages etc..


def propose(send_id1, send_id2): #starts the leader election 
    global ballot_num
    ballot_num[0] += 1
    print("Sending prepare")
    client_socket.send(f"prepare {ballot_num} {client_id} {send_id1} {send_id2}".encode())
    return

def promise(bal, proposer):
    client_socket.send(f"promise {bal} {proposer} {client_id}".encode())

def accept(ballot_num, promiser):
    client_socket.send(f"accept {ballot_num} {promiser} {client_id}".encode())

def handle_messages(): #handles receiving prepare messages etc..
    global curr_leader
    message = client_socket.recv(1024).decode()
    if message.startswith('prepare'):
        parts  = message.split()
        bal = ast.literal_eval(''.join(parts[1:3]))
        proposer = parts[3]
        print(bal, ballot_num)
        if bal >= ballot_num:
            curr_leader = proposer
            print(f"{curr_leader} is the leader")
            promise(bal, proposer)
    elif message.startswith('promise'):

        # _, ballot_num, accept_num, accept_val, _, promiser = message.split()
        # accept(ballot_num, promiser)
        curr_leader = client_id
        print(f"Leader election is complete, {curr_leader} is leader")

        
        

    return


def create_context(message):
    if curr_leader == client_id:
        _ , context_id = message.split()
        key_value_store[context_id] = ""
    else:
        message = f"{message} {client_id} {curr_leader}"
        send_to_leader(message)

def query_context(message):
    global leader_queue, query_queue
    _ , context_id, query = message.split()
    if curr_leader == client_id:
        leader_queue.put((context_id, query))
    else:
        query_queue.put((context_id, query))
        send_to_leader(message)

def send_to_leader(message):
    message = "{message} {client_id} {curr_leader}"
    client_sockets[curr_leader - 1].send(message.encode())
    
def choose_response(message):
    return

def view_context(message):
    _, context_id = message.split()
    print(key_value_store[context_id])

def view_all_context():
    print(key_value_store)


if __name__ == "__main__":
    global port
    # global client_id
    if len(sys.argv) < 3:
        sys.exit(1)
    port = int(sys.argv[2])
    client_id = int(sys.argv[1])
    ballot_num[1] = client_id
    start_client()
    if(client_id == 3): #This will be the initial leader proposer
        propose(1,2)
    while running:
        message = input()
        if message.startswith('create'):
            threading.Thread(target=create_context, args=(message,)).start()
        if message.startswith('query'):
            threading.Thread(target=query_context, args=(message,)).start()
        if message.startswith('choose'):
            threading.Thread(target=choose_response, args=(message,)).start()
        if message.startswith('view '):
            view_context(message)
        if message == "viewall":
            view_all_context()     
        elif message == 'exit':
            running = False
            handle_exit()