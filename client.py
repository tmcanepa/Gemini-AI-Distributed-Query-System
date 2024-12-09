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

load_dotenv()

gemini_api = os.getenv("API_KEY")

genai.configure(api_key=gemini_api)
model = genai.GenerativeModel("gemini-1.5-flash")

key_value_store = {}
decide_count = defaultdict(lambda: 0)
leader_queue = queue.Queue()
operation_queue = queue.Queue()
running = True
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_id = 0
curr_leader = 0
ballot_num = [0, 0, 0]
timeout_flag_proposal = True
timeout_flag_forward = True
timeout_flag_inherit_kvs = True
consensus_flag = True
key_value_store_lock = threading.Lock()
gemini_answers_lock = threading.Lock()
consensus_flag_lock = threading.Lock()
print_lock = threading.Lock()
queue_lock = threading.Lock()
decide_count_lock = threading.Lock()

gemini_answers = defaultdict(lambda: [None] * 3)

def handle_exit():
    client_socket.close()
    sys.stdout.flush()
    os._exit(0)

def start_client():
    global client_socket
    global client_id
    client_socket.connect(('127.0.0.1', port))
    port_num = client_socket.getsockname()[1]
    client_socket.send((json.dumps({
        "client_id": client_id,
        "port_num": port_num,
        "ballot_num": ballot_num,
        "kvs" : key_value_store
    })).encode())
    response = client_socket.recv(1024).decode()
    _, response_json = server.is_json(response)
    if response_json["type"] != "Success":
        print("Failure to send server clientid")
    # sendid1, sendid2 = get_other_server_ids()
    # if curr_leader != client_id:
    #     inherit_kvs(ballot_num, sendid1, sendid2)
    threading.Thread(target=handle_messages).start()

def get_other_server_ids():
    send_id1 = (client_id + 1) % 3
    send_id2 = (client_id + 2) % 3
    if send_id1 == 0:
        send_id1 = 3
    if send_id2 == 0:
        send_id2 = 3
    return send_id1, send_id2

def propose():
    global ballot_num
    send_id1, send_id2 = get_other_server_ids()
    ballot_num[0] += 1
    ballot_num[1] = client_id
    client_socket.send((json.dumps({
        "type": "prepare",
        "ballot_num": ballot_num,
        "send_id1": send_id1,
        "send_id2": send_id2,
        "proposer": client_id,
        "kvs": key_value_store
    })+'\n').encode())
    with print_lock:
        print(f"Sending PREPARE {ballot_num} to ALL")
    threading.Timer(20, timed_out, args=("proposal",)).start()
    return

def promise(bal, proposer):
    client_socket.send((json.dumps({
        "type": "promise",
        "proposer": proposer,
        "ballot_num": bal,
        "promiser": client_id,
        "curr_leader": curr_leader,
        "kvs": key_value_store
    })+'\n').encode())
    with print_lock:
        print(f"Sending PROMISE {bal} to Server {proposer}")

def accept(promiser, bal):
    client_socket.send((json.dumps({
        "type": "accept",
        "promiser": promiser,
        "client_id": client_id,
        "ballot_num": bal,
        "kvs": key_value_store
    })+'\n').encode())
    with print_lock:
        print(f"Sending ACCEPT {bal} to Server {promiser}")

def delete_from_operation_queue(input1, input2, input3):
    global operation_queue
    query_to_remove = (input1, input2, input3)
    temp_list = list(operation_queue.queue)
    if query_to_remove in temp_list:
        temp_list.remove(query_to_remove)
    with queue_lock:
        operation_queue.queue.clear()
        for item in temp_list:
            if not isinstance(item, tuple) or len(item) != 3:
                print(f"Invalid item: {item}")
                continue
            try:
                operation_queue.put(item)
            except Exception as e:
                print(f"Failed to add item: {e}")
    return

def ask_gemini(query, context_id, query_from):
    global gemini_answers
    response = str(model.generate_content(query).text)
    return response

def build_operation(message) -> str:
    # print(f"building an operation using json {json.dumps(message)}")
    if message['input_type'] == "query":
        return f"query {message['context_id']} {message['query']}"
    elif message['input_type'] == "choose":
        return f"choose {message['context_id']} {message['answer_num']}"
    elif message['input_type'] == "create":
        return f"create {message['context_id']}"
    elif message["input_type"] == "inherit_lock":
        return f"inherit_lock {message["query_from"]} Null"
    else:
        print(f"You should not be building right now with {message['type']}")
        return ""
    
# def inherit_kvs(bal, sendid1, sendid2):
#     global key_value_store
#     # print("Trying to inherit updated key value store")
#     client_socket.send((json.dumps({
#         "type": "inherit_kvs",
#         "ballot_num": bal,
#         "send_id1": sendid1,
#         "send_id2": sendid2,
#         "client_id": client_id,
#     })+'\n').encode())
#     print(f"Sending INHERIT {bal} to ALL")
#     threading.Timer(25, timed_out, args=("inherit_kvs",)).start()
#     return

def handle_messages():
    global curr_leader,timeout_flag_proposal,timeout_flag_forward,leader_queue,operation_queue,consensus_flag
    global timeout_flag_inherit_kvs,ballot_num, key_value_store,gemini_answers, decide_count
    buffer = ""
    while True:
        message = client_socket.recv(1024).decode()
        if not message:
            with print_lock:
                print("Socket has been shutdown. Exiting.")
            handle_exit()
        buffer += message
        while '\n' in buffer:
            message, buffer = buffer.split('\n', 1)
            bool_json, message = server.is_json(message)
            # print(f"Received {message}")
            bal = message['ballot_num']
            if bal[2] > ballot_num[2] + 1:
                print("UPDATING KVS FOR BALLOT", bal)
                key_value_store = message['kvs']
            if bool_json:
                message_type = message['type']
                if message_type == "prepare":
                    bal = message['ballot_num']
                    if bal[2] >= ballot_num[2]: #might be allowed to be equal for leader election
                        # print("UPDATING KVS FOR BALLOT", bal, "NEW KVS", message['kvs'])
                        # print("NEW KVS", message['kvs'])
                        key_value_store = message['kvs']
                    proposer = message['proposer']
                    with print_lock:
                        print(f"Received PREPARE {bal} from Server {proposer}")
                    # print("Ballots: ", bal, ballot_num)
                    if bal >= ballot_num:
                        if bal[2] < ballot_num[2]:
                            print("HES TRYING TO INHERIT")
                            promise(ballot_num, proposer)
                        else:
                            ballot_num = bal
                            promise(bal, proposer)
                elif message_type == "promise":
                    bal = message['ballot_num']
                    timeout_flag_proposal = False
                    curr_leader = bal[1]
                    with decide_count_lock:
                        decide_count = defaultdict(lambda: 0)
                    if curr_leader == client_id:
                        leader_queue = operation_queue
                    else:
                        operation_queue.queue.clear()
                    promiser = message['promiser']
                    time.sleep(.01)
                    # print(f"curr leader = {curr_leader} and leader queue = {leader_queue.queue}")
                    if bal[2] >= ballot_num[2]:
                        # print("UPDATING KVS FOR BALLOT", bal, "NEW KVS", message['kvs'])
                        key_value_store = message['kvs']
                        curr_leader = message['curr_leader']
                    with print_lock:
                        print(f"Received PROMISE {bal} from Server {promiser}")

                    accept(promiser, bal)
                elif message_type == "accept":
                    bal = message['ballot_num']
                    curr_leader = bal[1]
                    # print(f"curr leader = {curr_leader}")
                    with decide_count_lock:
                        decide_count = defaultdict(lambda: 0)
                    if bal[2] >= ballot_num[2]: #might be allowed to be equal for leader election
                        # print("UPDATING KVS FOR BALLOT", bal)
                        # print("NEW KVS", message['kvs'])
                        key_value_store = message['kvs']
                    with print_lock:
                        print(f"Received ACCEPT {bal} from Server {curr_leader}")
                    # print(f"{curr_leader} is the leader")
                elif message_type == "propose_query":
                    send_id1, send_id2 = get_other_server_ids()
                    context_id = message['context_id']
                    query_from = message['query_from'] #This is where the original query came from
                    client_id_local = message['client_id'] #This is the leader that received the forwarded the operation
                    query = message['query']
                    bal = message['ballot_num']
                    if bal >= ballot_num:
                        ballot_num = bal
                        #update accept num and accept val
                        pass
                    with print_lock:
                        print(f"Received ACCEPTED {bal} query {context_id} {query} from Server {client_id_local}")
                    LLM_answer = ask_gemini(key_value_store[context_id] + query, context_id, query_from)
                    client_socket.send((json.dumps({
                        "type": "decide_query",
                        "context_id": context_id,
                        "query_from": query_from,
                        "query": query,
                        "send_id1": send_id1,
                        "send_id2": send_id2,
                        "client_id": client_id,
                        "ballot_num": bal,
                        "LLM_answer" : LLM_answer,
                        "kvs": key_value_store
                    }) + '\n').encode())
                    with print_lock:
                        print(f"Sending DECIDE {bal} query {context_id} {query} to ALL")
                elif message_type == "propose_choose":
                    send_id1, send_id2 = get_other_server_ids()
                    context_id = message['context_id']
                    query_from = message['query_from']
                    LLM_answer = message['LLM_answer']
                    client_id_local = message['client_id']
                    bal = message['ballot_num']
                    answer_num = message['answer_num']
                    if bal >= ballot_num:
                        ballot_num = bal
                        pass
                    with print_lock:
                        print(f"Received ACCEPTED {bal} answer {answer_num} {LLM_answer} from Server {client_id_local}")
                    client_socket.send((json.dumps({
                        "type": "decide_choose",
                        "context_id": context_id,
                        "query_from": query_from,
                        "LLM_answer": LLM_answer,
                        "send_id1": send_id1,
                        "send_id2": send_id2,
                        "client_id": client_id,
                        "ballot_num": bal,
                        "answer_num" : answer_num,
                        "kvs": key_value_store
                    }) + '\n').encode())
                    with print_lock:
                        print(f"Sending DECIDE {bal} answer {answer_num} {LLM_answer} to ALL")
                elif message_type == "propose_create":
                    bal = message['ballot_num']
                    if bal >= ballot_num:
                        ballot_num = bal
                        pass
                    send_id1, send_id2 = get_other_server_ids()
                    context_id = message['context_id']
                    client_id_local = message['client_id']
                    with print_lock:
                        print(f"Received ACCEPTED {bal} create {context_id} from Server {client_id_local}")
                    client_socket.send((json.dumps({
                        "type": "decide_create",
                        "context_id": context_id,
                        "send_id1": send_id1,
                        "send_id2": send_id2,
                        "client_id": client_id,
                        "ballot_num": bal,
                        "kvs": key_value_store
                    }) + '\n').encode())
                    with print_lock:
                        print(f"Sending DECIDE {bal} create {context_id} to ALL")
                elif message_type == "decide_query":
                    bal = message['ballot_num']
                    with decide_count_lock:
                        decide_count[bal[2]] = decide_count[bal[2]] + 1
                        LLM_answer = message['LLM_answer']
                        context_id = message['context_id']
                        query_from = message['query_from'] #where message originated
                        candidate = message["client_id"] #Where message came from
                        query = message['query']
                        with consensus_flag_lock:
                            if curr_leader == client_id and decide_count[bal[2]] == 2:
                                    leader_queue.get() #Remove from leader queue now that query is decided
                                    consensus_flag = True
                            with print_lock:
                                print(f"Received DECIDE {bal} query {context_id} {query} from Server {candidate}")
                        with key_value_store_lock and gemini_answers_lock:
                            gemini_answers[context_id][candidate-1] = LLM_answer
                            if query_from == client_id:
                                with print_lock:
                                    print(f"Context {context_id} - Candidate {candidate}: {LLM_answer}")
                            if decide_count[bal[2]] == 2:
                                key_value_store[context_id] += f"QUERY: {query}\n"
                                with print_lock:
                                    print(f"NEW Query on {context_id} with {key_value_store[context_id]}")
                elif message_type == "decide_choose":
                    bal = message['ballot_num']
                    with decide_count_lock:
                        decide_count[bal[2]] = decide_count[bal[2]] + 1
                        context_id = message['context_id']
                        LLM_answer = message['LLM_answer']
                        query_from = message['query_from']
                        answer_num = message['answer_num']
                        client_id_local = message['client_id']
                        with consensus_flag_lock:
                            if curr_leader == client_id and decide_count[bal[2]] == 2:
                                    leader_queue.get() #Remove from leader queue now that query is decided
                                    consensus_flag = True
                            with print_lock:
                                print(f"Received DECIDE {bal} answer {answer_num} {LLM_answer} from Server {client_id_local}")
                        with key_value_store_lock:
                            if decide_count[bal[2]] == 2:
                                if len(LLM_answer) >= 7 and LLM_answer[:7] == "ANSWER:":
                                    key_value_store[context_id] += f"{LLM_answer}"
                                else:
                                    key_value_store[context_id] += f"ANSWER: {LLM_answer}"
                                gemini_answers[context_id] = [None] * 3
                                with print_lock:
                                    print(f"Chosen ANSWER on {context_id} with {LLM_answer}")
                elif message_type == "decide_create":
                    bal = message['ballot_num']
                    with decide_count_lock:
                        decide_count[bal[2]] = decide_count[bal[2]] + 1
                        context_id = message['context_id']
                        client_id_local = message['client_id']
                        with consensus_flag_lock:
                            if curr_leader == client_id and decide_count[bal[2]] == 2:
                                    leader_queue.get() #Remove from leader queue now that query is decided
                                    consensus_flag = True
                            with print_lock:
                                print(f"Received DECIDE {bal} create {context_id} from Server {client_id_local}")
                        with key_value_store_lock:
                            if decide_count[bal[2]] == 2:
                                key_value_store[context_id] = ""
                                with print_lock:
                                    print(f"New CONTEXT {context_id}")
                elif message_type == "forward_to_leader":
                    message["type"] = "ack_leader_queued"
                    client_id_local = message["client_id"]
                    LLM_answer = message["query"]
                    input_type = message['input_type']
                    context_id = message["context_id"]
                    additional_print = message["additional_print"]
                    with print_lock:
                        print(f"Received FORWARD {ballot_num} {input_type} {context_id} {additional_print} from Server {client_id_local}")
                    client_socket.send((json.dumps(message) + '\n').encode())
                    with print_lock:
                        print(f"Sending ACK {ballot_num} {input_type} {context_id} {additional_print}to Server {client_id_local}") #the lack of gap is intentional for print format
                    operation = build_operation(message)
                    threading.Thread(target=create_query_choose_context, args=(operation, client_id_local, LLM_answer)).start()
                elif message_type == "ack_leader_queued":
                    timeout_flag_forward = False
                    input_type = message['input_type']
                    client_id_local = message["client_id"]
                    context_id = message["context_id"]
                    query = message["query"]
                    if input_type == "query":
                        delete_from_operation_queue(context_id, query, client_id_local)
                    if input_type == "create":
                        delete_from_operation_queue("create", context_id, "create")
                    if input_type == "choose":
                        delete_from_operation_queue(context_id, query, client_id_local)
            else:
                print(f"Received non-JSON message: {message}")
                handle_exit()

def timed_out(type):
    global timeout_flag_forward, timeout_flag_proposal, consensus_flag, timeout_flag_inherit_kvs,curr_leader,decide_count
    print("decide count:", list(decide_count))
    if type == "operation": #forwarded message to leader timesout
        if timeout_flag_forward:
            with print_lock:
                print("TIMEOUT")
            propose()
        timeout_flag_forward = True
    if type == "proposal": #leader election times out
        if timeout_flag_proposal:
            print("TIMEOUT")
            propose()
        timeout_flag_proposal = True
    if type == "consensus": #leader starting a consensus times out
        if not consensus_flag:
            print("TIMEOUT")
            curr_leader = 0
            propose()
        consensus_flag = True #Technically consensus was not reached but this is true so leader queue can reattempt
    if type == "inherit_kvs": #Trying to inherit kvs times out
        if timeout_flag_inherit_kvs:
            print("TIMEOUT")
            propose()
        timeout_flag_inherit_kvs = True
    return

def create_query_choose_context(message, client_id_local, LLM_answer):
    global leader_queue, operation_queue
    # print(f"Trying to add to leader queue message = {message}")
    parts = message.split()
    input_type = parts[0]
    if input_type == "create":
        input1 = "create"
        input2 = parts[1]
        input3 = "create"
        if input2 in key_value_store:
            print(f"Context id = {input2} has already been created")
            return
    elif input_type == "query":
        input1 = parts[1] #context id
        input2 = " ".join(parts[2:]) #query
        input3 = client_id_local #where the query came from
        with key_value_store_lock:
            if input1 not in key_value_store:
                print(f"Could not process query = {input2} for context id = {input1} because that context id has not been created yet!")
                return
        with gemini_answers_lock:
            # print(f"gemini answers = {gemini_answers[input1]}")
            if gemini_answers[input1] != [None, None,None]:  
                print(f"Sorry we can not process another query for context id = {input1} until an answer has been chosen for the previous query!")
                return
    elif input_type == "choose":
        input1 = parts[1]
        input3 = "choose"
        with key_value_store_lock and gemini_answers_lock:
            if input1 not in key_value_store:
                print(f"Could not choose from context id = {input1} because that context id has not been create yet!")
                return
            if client_id_local == client_id:
                if int(parts[2]) < 1 or int(parts[2]) > len(gemini_answers[parts[1]]) or gemini_answers[parts[1]] == None:
                    print(f"Could not choose from context id = {input1} for answer = {parts[2]} because that is not a valid answer to choose from!")
                    return
                # input2 = gemini_answers[parts[1]][int(parts[2])-1]
                # input2 = LLM_answer
            input2 = parts[2] #answer_num
    if curr_leader == 0:
        operation_queue.put((input1, input2, input3))
        propose()
    elif curr_leader == client_id:
        leader_queue.put((input1, input2, input3))
        # print(f"just added to leader queue ({input1} {input2} {input3})")
    else:
        operation_queue.put((input1, input2, input3))
        send_to_leader(message)
        threading.Timer(20, timed_out, args=("operation",)).start()
    return

def send_to_leader(message):
    parts = message.split()
    if curr_leader != 0:
        context_id = parts[1]
        query = "Null"
        answer_num = 0
        # query_from = client_id
        additional_print = ""
        input_type = parts[0]
        if input_type == "query":
            query = " ".join(parts[2:])
            additional_print = query + " "
        elif input_type == "choose":
            answer_num = parts[2]
            query = gemini_answers[context_id][int(answer_num)-1] #This is here just to save space, it is a LLM_answer not a query
            input_type = "answer" #this is here just for printing purpose
            additional_print = query + " "
        # elif input_type == "inherit_lock":
        #     query_from = parts[1]
        client_socket.send((json.dumps({
            "type": "forward_to_leader",
            "client_id": client_id,
            "curr_leader": curr_leader,
            "input_type": parts[0],
            "context_id": context_id,
            "query": query,
            "answer_num": answer_num,
            "ballot_num": ballot_num,
            "kvs": key_value_store,
            "additional_print" : additional_print
        }) + '\n').encode())
        print(f"Sending FORWARD {ballot_num} {input_type} {context_id} {additional_print}to Server {curr_leader}") #the gap is intentioinal

def view_context(message):
    _, context_id = message.split()
    with key_value_store_lock:
        print(key_value_store[context_id])

def view_all_context():
    with key_value_store_lock:
        print(key_value_store)

def consensus_operation(input1, input2, input3):
    send_id1, send_id2 = get_other_server_ids()
    # print(f"Starting consensus op on {input1} {input2}")
    if input3 == "create":
        context_id = input2
        ballot_num[2] += 1
        client_socket.send((json.dumps({
            "type": "propose_create",
            "context_id": context_id,
            "send_id1": send_id1,
            "send_id2": send_id2,
            "client_id": client_id,
            "ballot_num": ballot_num,
            "kvs": key_value_store
        })+'\n').encode())
        print(f"Sending ACCEPTED {ballot_num} create {context_id} to ALL")
    elif input3 == "choose":
        context_id = input1
        answer_num = input2
        LLM_answer = gemini_answers[context_id][int(answer_num)-1]
        query_from = input3
        ballot_num[2] += 1
        decide_count[ballot_num[2]] = 0
        client_socket.send((json.dumps({
            "type": "propose_choose",
            "context_id": context_id,
            "query_from": query_from,
            "LLM_answer": LLM_answer,
            "send_id1": send_id1,
            "send_id2": send_id2,
            "client_id": client_id,
            "ballot_num": ballot_num,
            "answer_num": answer_num,
            "kvs": key_value_store
        })+'\n').encode())
        print(f"Sending ACCEPTED {ballot_num} answer {answer_num} {LLM_answer} to ALL")
    else:
        context_id = input1
        query = input2
        ballot_num[2] += 1
        decide_count[ballot_num[2]] = 0
        client_socket.send((json.dumps({
            "type": "propose_query",
            "context_id": context_id,
            "query_from": input3,
            "query": query,
            "send_id1": send_id1,
            "send_id2": send_id2,
            "client_id": client_id,
            "ballot_num": ballot_num,
            "kvs": key_value_store
        })+'\n').encode())
        print(f"Sending ACCEPTED {ballot_num} query {context_id} {query} to ALL")
    return

def leader_queue_thread():
    global curr_leader, leader_queue, operation_queue, consensus_flag
    while True:
        with consensus_flag_lock:
            if curr_leader == client_id and not leader_queue.empty() and consensus_flag:
                consensus_flag = False
                input1, input2, input3 = leader_queue.queue[0]
                print(f"Handling a leader operation from {input2}")
                # print(f"leader_queue = {list(leader_queue.queue)}")
                if input1 == "inherit_lock":
                    client_socket.send((json.dumps({
                            "type": "ack_inherit_kvs",
                            "ballot_num": ballot_num,
                            "kvs": key_value_store,
                            "curr_leader": client_id,
                            "client_id": client_id,
                            "return_user": input2,
                            "kvs": key_value_store
                        })+'\n').encode())
                    print(f"Sending ACK_INHERIT {ballot_num} to Server {input2}")
                else:
                    consensus_operation(input1, input2, input3)
                    threading.Timer(20, timed_out, args=("consensus",)).start()

def valid_input(message):
    parts = message.split()
    if message == "":
        return False
    elif len(parts) == 1 and (parts[0] == "viewall" or parts[0] == "exit"):
        return True
    elif len(parts) == 2 and (parts[0] == "view" or parts[0] == "create") and parts[1].isdigit():
        return True
    elif len(parts) == 3 and (parts[0] == "choose") and parts[1].isdigit() and parts[2].isdigit():
        return True
    elif len(parts) > 2 and (parts[0] == "query") and parts[1].isdigit():
        return True
    else:
        return False

if __name__ == "__main__":
    global port
    if len(sys.argv) < 3:
        sys.exit(1)
    port = int(sys.argv[2])
    client_id = int(sys.argv[1])
    ballot_num[1] = client_id
    start_client()
    threading.Thread(target=leader_queue_thread).start()
    while running:
        message = input()
        if valid_input(message):
            if message.startswith('create') or message.startswith('query') or message.startswith('choose'):
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
