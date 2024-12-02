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
from collections import defaultdict



load_dotenv()

gemini_api = os.getenv("API_KEY")

genai.configure(api_key=gemini_api)
model = genai.GenerativeModel("gemini-1.5-flash")



key_value_store = {}
leader_queue = queue.Queue()
operation_queue = queue.Queue()
running = True
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_id = 0
curr_leader = 0
ballot_num = [0,0]
timeout_flag_proposal = True
timeout_flag_query = True
key_value_store_lock = threading.Lock()
#This is here for testing purposes
key_value_store[1] = "" 
curr_leader = 1
gemini_answers = defaultdict(list)


def handle_exit():
    client_socket.close()
    sys.stdout.flush()
    sys.exit(0)

    

def start_client(): #Connect to the coordinator and give it your id and port number 
    global client_socket
    global client_id
    client_socket.connect(('127.0.0.1', port))
    port_num = client_socket.getsockname()[1]
    client_socket.send(f"{client_id} {port_num}\n".encode())
    response = client_socket.recv(1024).decode()
    if response != "Success":
        print("Failure to send server clientid")
    threading.Thread(target = handle_messages, ).start() #Needed for receiving prepare messages etc..
def get_other_server_ids():
    send_id1 = (client_id + 1)%3
    send_id2 = (client_id + 2)%3
    if send_id1 == 0:
        send_id1 = 3
    if send_id2 == 0:
        send_id2 = 3
    return send_id1,send_id2

def propose(): #starts the leader election 
    global ballot_num
    send_id1, send_id2 = get_other_server_ids()
    ballot_num[0] += 1
    print("Sending prepare")
    client_socket.send(f"prepare {ballot_num} {send_id1} {send_id2} {client_id}\n".encode())
    threading.Timer(10, timed_out, args = ("proposal",)).start()
    return

def promise(bal, proposer):
    client_socket.send(f"promise {proposer} {bal} {client_id}\n".encode())

def accept(promiser):
    client_socket.send(f"accept {promiser} {client_id}\n".encode())

def delete_from_operation_queue(context_id, query, query_from):
    global operation_queue
    query_to_remove = (context_id, query, query_from)
    temp_list = list(operation_queue.queue)
    if query_to_remove in temp_list:
        temp_list.remove(query_to_remove)
    with operation_queue.mutex:
        operation_queue.queue.clear()
        for item in temp_list:
            operation_queue.put(item)
    return

def ask_gemini(query, context_id, query_from):
    global gemini_answers
    # print(f"Asking gemini {query}")
    # print(f"In contex {context_id} and the original question came from {query_from}")
    response = model.generate_content(query)
    if query_from == client_id:
        gemini_answers[context_id].append(query)
        print(f"Answer{len(gemini_answers[context_id])} = {query}")
    else:
        client_socket.send(f"GEMINI {query_from} {context_id} {response.text}\n".encode())
    return

def fix_paragraph_formatting(paragraph) -> str:
    return " ".join(paragraph).replace("[", "").replace("]", "").replace(",", "").replace("'", "").replace('"', "").strip()

def handle_messages(): #handles receiving prepare,promise, and forwarded input from other nonleader servers, consensus for queries
    global curr_leader
    global timeout_flag_proposal
    global timeout_flag_query
    global leader_queue
    global operation_queue
    while True:
        message = client_socket.recv(1024).decode()
        parts = message.split()
        print(f"Received {message}")
        if not message:
            handle_exit()
        if message.startswith('prepare'):
            # print(f"I received message {message}")
            bal = ast.literal_eval(''.join(parts[1:3]))
            proposer = parts[-1]
            print("Ballots: ", bal, ballot_num)
            if bal >= ballot_num:
                promise(bal, proposer)
        elif message.startswith('promise'):
            timeout_flag_proposal = False
            curr_leader = client_id
            leader_queue = operation_queue
            promiser = parts[4]
            accept(promiser)
            print(f"Leader election is complete, {curr_leader} is leader")
        elif message.startswith('accept'):
            curr_leader = int(parts[2])
            print(f"{curr_leader} is the leader")
        elif message.startswith('create'):
            context_id = parts[1]
            leader_queue.put(("create", context_id, "null"))
        elif message.startswith('query'):
            query_from = parts[0]
            context_id = parts[3]
            query = parts[4:]
            leader_queue.put((context_id, query, query_from))
            #According to https://piazza.com/class/m1l88ck8vw21ia/post/261 acknowledge that leader has received forwarded message
            #so that the acceptor can forgot about the query. If leader crashes, we do not have to recover leader queue
            client_socket.send(f"ack_leader_queued {query_from} {context_id} {query}\n".encode()) 
        elif message.startswith('propose_query'):
            send_id1, send_id2 = get_other_server_ids()
            context_id = parts[1]
            query_from = parts[2]
            query = parts[3:-3]
            client_socket.send(f"decide_query {context_id} {query_from} {query} {send_id1} {send_id2} {client_id}\n".encode())
        elif message.startswith('propose_create'):
            send_id1, send_id2 = get_other_server_ids()
            context_id = parts[1]
            client_socket.send(f"decide_create {context_id} {send_id1} {send_id2} {client_id}\n".encode())
        elif message.startswith('decide_query'):
            context_id = parts[1]
            query_from = parts[2]
            query = fix_paragraph_formatting(parts[3:-3])
            len_query = len(query)
            with key_value_store_lock:
                recent_query = "" if key_value_store[int(context_id)] == "" else  key_value_store[int(context_id)][-1*len_query-1:-1]
                print(f"Compare queries {query} + {recent_query}")
                # print(f"Okay just look at kvs {key_value_store[int(context_id)]}")
                if len(key_value_store[int(context_id)]) < len_query or (len(key_value_store[int(context_id)]) > len_query and recent_query != query):
                        key_value_store[int(context_id)] += f"QUERY: {query}\n"
                        ask_gemini(key_value_store[int(context_id)], context_id, query_from)
                        print(f"You just added to key value store!! {query}")
        elif message.startswith('decide_create'):
            context_id = parts[1]
            key_value_store[int(context_id)] = ""
            print(f"You just created a key value store!! {context_id}")
        elif message.startswith('choose'):
            pass
        elif message.startswith('ack_leader_queued'):
            #we are free to clear the query from our queue as the leader received it
            timeout_flag_query = False
            query_from = parts[1]
            context_id = parts[2]
            query = parts[3:]
            delete_from_operation_queue(context_id, query, query_from)
        elif message.startswith('GEMINI'):
            context_id = parts[2]
            response  = fix_paragraph_formatting(parts[3:])
            # print(f"Checking gemini format {response}")
            gemini_answers[context_id].append(response)
            print(f"Answer{len(gemini_answers[context_id])} = {response}")

def timed_out(type):
    global timeout_flag_query
    global timeout_flag_proposal
    if type == "operation":
        if timeout_flag_query:
            print("Socket for query Timed Out")
            propose()
        timeout_flag_query = True
    if type == "proposal":
        if timeout_flag_proposal:
            print("Socket for proposal Timed Out")
            propose()
        timeout_flag_proposal = True
    return


def create_or_query_context(message):
    global leader_queue, operation_queue
    parts = message.split()
    type = parts[0]
    if type == "create":
        input1 = "create"
        input2 = parts[1]
    else:
        input1 = parts[1]
        input2 = parts[2:]
    if curr_leader == 0:
        operation_queue.put((input1, input2, client_id))
        propose()
    elif curr_leader == client_id:
        leader_queue.put((input1, input2, client_id))
    else:
        operation_queue.put((input1, input2, client_id))
        send_to_leader(message)
        threading.Timer(10, timed_out, args = ("operation",)).start()
    return


def send_to_leader(message):
    if curr_leader != 0:
        client_socket.send(f"{client_id} {curr_leader} {message}\n".encode())
    
def choose_response(message):
    return

def view_context(message):
    _, context_id = message.split()
    print(key_value_store[int(context_id)])

def view_all_context():
    print(key_value_store)

def consensus_operation(input1, input2, query_from):
    send_id1, send_id2 = get_other_server_ids()
    print(f"Starting consensus op on {input1} {input2}")
    if input1 == "create":
        context_id = input2
        client_socket.send(f"propose_create {context_id} {send_id1} {send_id2} {client_id}\n".encode())
    else:
        context_id = input1
        query = input2
        client_socket.send(f"propose_query {context_id} {query_from} {query} {send_id1} {send_id2} {client_id}\n".encode())

def leader_queue_thread(): #Handles concensus for queries in the leader queue.
    global curr_leader, leader_queue, operation_queue
    while True:
        # time.sleep(5)
        # print(curr_leader, leader_queue.qsize(), operation_queue.qsize())
        if curr_leader == client_id and not leader_queue.empty():
            print("Handling a leader query")
            input1, input2, query_from = leader_queue.get()
            consensus_operation(input1, input2,query_from)


if __name__ == "__main__":
    global port
    # global client_id
    if len(sys.argv) < 3:
        sys.exit(1)
    port = int(sys.argv[2])
    client_id = int(sys.argv[1])
    ballot_num[1] = client_id
    start_client()
    threading.Thread(target = leader_queue_thread).start()
    while running:
        message = input()
        if message.startswith('create') or message.startswith('query'):
            threading.Thread(target=create_or_query_context, args=(message,)).start()
        if message.startswith('choose'):
            threading.Thread(target=choose_response, args=(message,)).start()
        if message.startswith('view '):
            view_context(message)
        if message == "viewall":
            view_all_context()     
        elif message == 'exit':
            running = False
            handle_exit()