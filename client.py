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
import json
import server

#https://piazza.com/class/m1l88ck8vw21ia/post/257
#This piazza post discusses ambiguity when making multiple queries on the same context
#For the sake of our project we have decided to simply not allow another query to be made until the first one has an answer chosen

#['[', ']', ',', "'", '"']
#remove outer brackets
#take off single quote and keep parsing until 



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
gemini_answers_lock = threading.Lock()
queue_lock = threading.Lock()

gemini_answers = defaultdict(list)
#This is here for testing purposes

def testing_purposes(test):
    global curr_leader
    if test:
        key_value_store['1'] = "" 
        curr_leader = 1



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

def delete_from_operation_queue(input1, input2, input3): #Shit this should handle all of create, query, choose
    global operation_queue
    print(f"Trying to delete from operation queue input1 = {input1} input2 = {input2} input3 =  {input3}")
    query_to_remove = (input1, input2, input3)
    temp_list = list(operation_queue.queue)
    print(f"temp_list  = {temp_list}")
    if query_to_remove in temp_list:
        print(f"Just deleted {query_to_remove} from operation queue!")
        temp_list.remove(query_to_remove)
    with queue_lock:
        operation_queue.queue.clear()
        for item in temp_list:
            print(f"tyring to add item = {item}")
            if not isinstance(item, tuple) or len(item) != 3:
                    print(f"Invalid item: {item}")
                    continue
            try:
                operation_queue.put(item)
                print(f"Queue contents: {list(operation_queue.queue)}")   
            except Exception as e:
                print(f"Failed to add item: {e}")
    return

def ask_gemini(query, context_id, query_from):
    global gemini_answers
    # print(f"Asking gemini {query}")
    # print(f"In contex {context_id} and the original question came from {query_from}")
    response = str(model.generate_content(query).text)
    print(f"This server found response {response}")
    if query_from == client_id:
        gemini_answers[context_id].append(response)
        print(f"Answer{len(gemini_answers[context_id])} = {response}")
    else:
        client_socket.send(f"GEMINI {query_from} {context_id} {response}\n".encode())
    return


def build_operation(message) -> str:
    print(f"building an operation using json {json.dumps(message)}")
    if message['input_type'] == "query":
        return f"query {message["context_id"]} {message["query"]}"
    elif message['input_type'] == "choose":
        return f"choose {message["context_id"]} {message["answer_num"]}"
    elif message['input_type'] == "create":
        return  f"create {message["context_id"]}"
    else:
        print(f"You should not be building right now with {message['type']}")
        return ""

def handle_messages(): #handles receiving prepare,promise, and forwarded input from other nonleader servers, consensus for queries
    global curr_leader
    global timeout_flag_proposal
    global timeout_flag_query
    global leader_queue
    global operation_queue
    buffer = "" #Need this because messages were being concatenated
    while True:
        message = client_socket.recv(1024).decode()
        buffer += message
        while '\n' in buffer:
            message, buffer = buffer.split('\n', 1)
            bool_json, message = server.is_json(message)
            print(f"Received {message}")
            if not bool_json:
                parts = message.split()
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
                elif message.startswith('propose_query'):
                    send_id1, send_id2 = get_other_server_ids()
                    context_id = parts[1]
                    query_from = parts[2]
                    query = " ".join(parts[3:-3])
                    client_socket.send(f"decide_query {context_id} {query_from} {query} {send_id1} {send_id2} {client_id}\n".encode())
                elif message.startswith('propose_choose'):
                    send_id1, send_id2 = get_other_server_ids()
                    context_id = parts[1]
                    query_from = parts[2]
                    LLM_answer = " ".join(parts[3:-3])
                    client_socket.send(f"decide_choose {context_id} {query_from} {LLM_answer} {send_id1} {send_id2} {client_id}\n".encode())
                elif message.startswith('propose_create'):
                    send_id1, send_id2 = get_other_server_ids()
                    context_id = parts[1]
                    client_socket.send(f"decide_create {context_id} {send_id1} {send_id2} {client_id}\n".encode())
                elif message.startswith('decide_query'):
                    context_id = parts[1]
                    query_from = parts[2]
                    query = " ".join(parts[3:-3])
                    len_query = len(query)
                    with key_value_store_lock and gemini_answers_lock:
                        recent_query = "" if key_value_store[context_id] == "" else  key_value_store[context_id][-1*len_query-1:-1]
                        print(f"Compare queries {query} + {recent_query}")
                        # print(f"Okay just look at kvs {key_value_store[context_id]}")
                        if len(key_value_store[context_id]) < len_query or (len(key_value_store[context_id]) > len_query and recent_query != query):
                                key_value_store[context_id] += f"QUERY: {query}\n"
                                ask_gemini(key_value_store[context_id], context_id, query_from)
                                print(f"You just added for context id  = {context_id} a query = {query} to key value store!!")
                elif message.startswith('decide_choose'):
                    context_id = parts[1]
                    LLM_answer = " ".join(parts[3:-3])
                    query_from = parts[2]
                    len_LLM_answer = len(LLM_answer)
                    with key_value_store_lock:
                        recent_answer = "" if len_LLM_answer > len(key_value_store[context_id]) else key_value_store[context_id][-1*len_LLM_answer-1:-1]
                        print(f"Compare queries {LLM_answer} + {recent_answer}")
                        # print(f"Okay just look at kvs {key_value_store[context_id]}")
                        if LLM_answer != recent_answer:
                                if len(LLM_answer) == 7 and LLM_answer[:7] == "ANSWER:":
                                    key_value_store[context_id] += f"{LLM_answer}\n"
                                else:
                                    key_value_store[context_id] += f"ANSWER: {LLM_answer}\n"
                                gemini_answers[context_id] = []
                                print(f"You just added answer to key value store!! {LLM_answer}")
                elif message.startswith('decide_create'):
                    context_id = parts[1]
                    with key_value_store_lock:
                        key_value_store[context_id] = ""
                    print(f"You just created a key value store!! {context_id}")
                elif message.startswith('GEMINI'):
                    context_id = parts[2]
                    response = " ".join(parts[3:])
                    # print(f"Checking gemini format {response}")
                    with gemini_answers_lock:
                        gemini_answers[context_id].append(response)
                        print(f"Answer{len(gemini_answers[context_id])} = {response}")
            else:
                message_type = message['type']
                print(f"Received a json string with type {message_type}!!!")
                if message_type =="forward_to_leader":
                    #received command from another server with fields "type, query_from, curr_leader, input_type,context_id, query, answer_num "
                    #message could be a query or create
                    message["type"] = "ack_leader_queued"
                    query_from = message["query_from"]
                    LLM_answer = message["query"]
                    client_socket.send((json.dumps(message) + '\n').encode()) 
                    operation = build_operation(message) #parse json to create operation
                    print(f"okay i built the operation {operation} ")
                    threading.Thread(target=create_query_choose_context, args=(operation,query_from, LLM_answer)).start()
                elif message_type == "ack_leader_queued":
                    #we are free to clear the operation from our queue as the leader received it
                    #It is in format type, query_from, curr_leader, input_type,context_id, query, answer_num
                    timeout_flag_query = False
                    input_type = message['input_type']
                    query_from = message["query_from"]
                    context_id = message["context_id"]
                    query = message["query"] #don't need to use " ".join here because it was sent in correct format
                    if input_type == "query":
                        delete_from_operation_queue(context_id, query, query_from)
                    if input_type == "create":
                        delete_from_operation_queue("create", context_id, "create")
                    if input_type == "choose":
                        delete_from_operation_queue(context_id, query, query_from)


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


def create_query_choose_context(message, query_from, LLM_answer):
    global leader_queue, operation_queue
    print(f"Trying to add to leader queue message = {message}")
    parts = message.split()
    input_type = parts[0]
    if input_type == "create":
        input1 = "create" #dummy variable
        input2 = parts[1] #context id
        input3 = "create" #dummy variable
    elif input_type == "query":
        input1 = parts[1] #context id
        input2 = " ".join(parts[2:]) #query string
        input3 = query_from #where query originated
        with key_value_store_lock:
            if input1 not in key_value_store:
                print(f"Could not process query = {input2} for context id = {input1} because that context id has not been created yet!")
                return
        with gemini_answers_lock: 
            if len(gemini_answers[input1]) > 0:
                print(f"Sorry we can not process another query for context id = {input1} until an answer has been chosen for the previous query!")
                return
    elif input_type == "choose":
        input1 = parts[1] #context id
        input3 = "choose"
        with key_value_store_lock and gemini_answers_lock:
            if input1 not in key_value_store:
                print(f"Could not choose from context id = {input1} because that context id has not been create yet!")
                return
            if input3 == client_id:
                if (int(parts[2]) < 1 or int(parts[2]) > len(gemini_answers[parts[1]])):
                    print(f"Could not choose from context id = {input1} for answer = {parts[2]} because that is not a valid answer to choose from!")
                    return
                input2 = gemini_answers[parts[1]][int(parts[2])-1] #LLM Answer
            else:
                input2 = LLM_answer
    if curr_leader == 0:
        operation_queue.put((input1, input2, input3))
        propose()
    elif curr_leader == client_id:
        leader_queue.put((input1, input2, input3))
        print(f"just added to leader queue ({input1} {input2} {input3})")
    else:

        operation_queue.put((input1, input2, input3))
        send_to_leader(message) 
        threading.Timer(10, timed_out, args = ("operation",)).start()
    return


def send_to_leader(message):
    parts = message.split()
    if curr_leader != 0:
        # client_socket.send(f"forward_to_leader {curr_leader} {message}\n".encode())
        parts = message.split()
        context_id = parts[1]
        query = "Null"
        answer_num = 0
        if parts[0] == "query":
            query = " ".join(parts[2:])
            # with key_value_store_lock:
            #     if context_id not in key_value_store:
            #         print(f"Could not process query = {query} for context id = {context_id} because that context id has not been created yet!")
            #         return
            # with gemini_answers_lock: 
            #     if len(gemini_answers[context_id]) > 0:
            #         print(f"Sorry we can not process another query for context id = {context_id} until an answer has been chosen for the previous query!")
            #         return
        elif parts[0] == "choose":
            answer_num = parts[2]
            # with key_value_store_lock and gemini_answers_lock:
            #     if context_id not in key_value_store:
            #         print(f"Could not choose from context id = {context_id} because that context id has not been created yet!")
            #         return
            #     if (int(answer_num) < 1 or int(answer_num) > len(gemini_answers[context_id])):
            #         print(f"Could not choose from context id = {context_id} for answer = {answer_num} because that is not a valid answer to choose from!")
            #         return
            query = gemini_answers[context_id][int(answer_num)-1]
        client_socket.send((json.dumps({
            "type" :  "forward_to_leader",
            "query_from" : client_id,
            "curr_leader" : curr_leader,
            "input_type" : parts[0],
            "context_id" : context_id,
            "query" : query,
            "answer_num" : answer_num}) + '\n').encode())

def view_context(message):
    _, context_id = message.split()
    with key_value_store_lock:
        print(key_value_store[context_id])

def view_all_context():
    with key_value_store_lock:
        print(key_value_store)

def consensus_operation(input1, input2, input3):
    send_id1, send_id2 = get_other_server_ids()
    print(f"Starting consensus op on {input1} {input2}")
    if input3 == "create":
        context_id = input2
        client_socket.send(f"propose_create {context_id} {send_id1} {send_id2} {client_id}\n".encode())
    elif input3 == "choose":
        context_id = input1
        LLM_answer = input2
        query_from  = input3
        client_socket.send(f"propose_choose {context_id} {query_from} {LLM_answer} {send_id1} {send_id2} {client_id}\n".encode())
    else: #query's input3 is a client id
        context_id = input1
        query = input2
        client_socket.send(f"propose_query {context_id} {input3} {query} {send_id1} {send_id2} {client_id}\n".encode())
        
    return

def leader_queue_thread(): #Handles concensus for queries in the leader queue.
    global curr_leader, leader_queue, operation_queue
    while True:
        if curr_leader == client_id and not leader_queue.empty():
            print("Handling a leader query")
            input1, input2, input3 = leader_queue.get()
            consensus_operation(input1, input2,input3)

def valid_input(message):
    parts = message.split()
    if message == "":
        return False
    elif len(parts) == 1 and (parts[0] == "viewall" or parts[0] == "exit"):
        return True
    elif len(parts) == 2 and (parts[0] == "view" or parts[0] == "create") and parts[1].isdigit():
        return True
    elif len(parts) == 3 and (parts[0] == "choose") and parts[1].isdigit() and parts[2].isdigit(): #
        return True
    elif len(parts) > 2 and (parts[0] == "query") and parts[1].isdigit():
        return True
    else:
        return False
    

if __name__ == "__main__":
    global port
    testing_purposes(True)
    if len(sys.argv) < 3:
        sys.exit(1)
    port = int(sys.argv[2])
    client_id = int(sys.argv[1])
    ballot_num[1] = client_id
    start_client()
    threading.Thread(target = leader_queue_thread).start()
    while running:
        message = input()
        if valid_input(message):
            if message.startswith('create') or message.startswith('query') or message.startswith('choose'): #create <context id>, query <context id> <query string>
                threading.Thread(target=create_query_choose_context, args=(message, client_id, "Null")).start()
            elif message.startswith('view '):
                view_context(message)
            elif message == "viewall":
                view_all_context()     
            elif message == 'exit':
                running = False
                handle_exit()
        else:
            print(f"{message} is not a valid input...")
            print("1. create <context id>")
            print("2. query <context id> <query string>")
            print("3. choose <context id> <response number>")
            print("4. view <context id>")
            print("5. viewall")
            print("6. exit")
