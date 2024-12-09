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
timeout_flag_query = True
consensus_flag = True
key_value_store_lock = threading.Lock()
gemini_answers_lock = threading.Lock()
consensus_flag_lock = threading.Lock()
queue_lock = threading.Lock()

ran_mess = None
stop_elect_flag = False

gemini_answers = defaultdict(list)

def testing_purposes(test):
    global curr_leader
    if test:
        key_value_store['1'] = ""
        curr_leader = 1

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
        "kvs": key_value_store
    })).encode())
    response = client_socket.recv(1024).decode()
    _, response_json = server.is_json(response)
    if response_json["type"] != "Success":
        print("Failure to send server clientid")
    threading.Thread(target=handle_messages).start()

def get_other_server_ids():
    send_id1 = (client_id + 1) % 3
    send_id2 = (client_id + 2) % 3
    if send_id1 == 0:
        send_id1 = 3
    if send_id2 == 0:
        send_id2 = 3
    # print(f"Found other ids {send_id1} and {send_id2}")
    return send_id1, send_id2

def propose():
    global ballot_num, stop_elect_flag
    send_id1, send_id2 = get_other_server_ids()
    ballot_num[0] += 1
    ballot_num[1] = client_id
    print("Sending PREPARE", ballot_num, "to ALL")
    client_socket.send((json.dumps({
        "type": "prepare",
        "ballot_num": ballot_num,
        "send_id1": send_id1,
        "send_id2": send_id2,
        "proposer": client_id,
        "kvs": key_value_store
    })+'\n').encode())
    if not stop_elect_flag:
        threading.Timer(10, timed_out, args=("proposal",)).start()
    # stop_elect_flag = False
    return

def promise(bal, proposer):
    print("Sending PROMISE", bal, "to SERVER", proposer)
    client_socket.send((json.dumps({
        "type": "promise",
        "proposer": proposer,
        "ballot_num": bal,
        "promiser": client_id,
        "kvs": key_value_store
    })+'\n').encode())

def accept(promiser):
    client_socket.send((json.dumps({
        "type": "accept",
        "promiser": promiser,
        "client_id": client_id,
        "ballot_num": ballot_num,
        "kvs": key_value_store
    })+'\n').encode())
    print("Sending ACCEPT", ballot_num, "to SERVER", promiser)
    # print("OPP QUEUE", operation_queue.queue)
    # print("LEADER QUEUE", leader_queue.queue)

def delete_from_operation_queue(input1, input2, input3):
    global operation_queue
    # print(f"Trying to delete from operation queue input1 = {input1} input2 = {input2} input3 =  {input3}")
    query_to_remove = (input1, input2, input3)
    temp_list = list(operation_queue.queue)
    # print(f"temp_list  = {temp_list}")
    if query_to_remove in temp_list:
        # print(f"Just deleted {query_to_remove} from operation queue!")
        temp_list.remove(query_to_remove)
    with queue_lock:
        operation_queue.queue.clear()
        for item in temp_list:
            # print(f"trying to add item = {item}")
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
    response = str(model.generate_content(query).text)
    print(f"This server found response {response}")
    if query_from == client_id:
        gemini_answers[context_id].append(response)
        print(f"Answer{len(gemini_answers[context_id])} for context {context_id} = {response}")
    else:
        client_socket.send((json.dumps({
            "type": "GEMINI",
            "query_from": query_from,
            "context_id": context_id,
            "response": response,
            "ballot_num": ballot_num,
            "kvs": key_value_store
        })+'\n').encode())
    return

def build_operation(message) -> str:
    # print(f"building an operation using json {json.dumps(message)}")
    if message['input_type'] == "query":
        return f"query {message['context_id']} {message['query']}"
    elif message['input_type'] == "choose":
        return f"choose {message['context_id']} {message['answer_num']}"
    elif message['input_type'] == "create":
        return f"create {message['context_id']}"
    else:
        print(f"You should not be building right now with {message['type']}")
        return ""

def handle_messages():
    global curr_leader,timeout_flag_proposal,timeout_flag_query,leader_queue,operation_queue,consensus_flag, ballot_num, key_value_store, gemini_answers, decide_count, stop_elect_flag, ran_mess
    buffer = ""
    while True:
        message = client_socket.recv(1024).decode()
        if not message:
            print("Socket has been shutdown. Exiting.")
            handle_exit()
        buffer += message
        while '\n' in buffer:
            message, buffer = buffer.split('\n', 1)
            bool_json, message = server.is_json(message)
            # print(f"Received {message}")
            bal = message['ballot_num']
            if bal[2] >= ballot_num[2] + 1:
                key_value_store = message['kvs']
            if bool_json:
                message_type = message['type']
                # print(f"Received a JSON message with type {message_type}!!!")
                if message_type == "update":
                    timeout_flag_proposal = False
                    key_value_store = message['kvs']
                    curr_leader = message['curr_leader']
                    stop_elect_flag = True
                    if curr_leader == client_id:
                        print("Putting", ran_mess, "in leader queue")
                        leader_queue.put((ran_mess, message['send_id1'], ran_mess))
                    else:
                        send_to_leader(ran_mess)

                if message_type == "prepare":
                    bal = message['ballot_num']
                    if bal[2] > ballot_num[2]:
                        key_value_store = message['kvs']
                    proposer = message['proposer']
                    # print("Ballots: ", bal, ballot_num)
                    print("Recieved PREPARE for", bal, "from SERVER", proposer)
                    if bal >= ballot_num:
                        if bal[2] < ballot_num[2]:
                            send_id1 = message['proposer']
                            print("Sending UPDATE to", send_id1, "for ballot", bal)
                            client_socket.send((json.dumps({
                                "type": "update",
                                "context_id": context_id,
                                "send_id1": send_id1,
                                "client_id": client_id,
                                "ballot_num": ballot_num,
                                "kvs": key_value_store,
                                "curr_leader": curr_leader
                            }) + '\n').encode())
                        else:
                            ballot_num = bal
                            promise(bal, proposer)
                elif message_type == "promise":
                    timeout_flag_proposal = False
                    curr_leader = client_id
                    leader_queue = operation_queue
                    promiser = message['promiser']
                    print("Received PROMISE", message['ballot_num'], "from SERVER", promiser)
                    accept(promiser)
                    # print(f"Leader election is complete, {curr_leader} is leader")
                elif message_type == "accept":
                    bal = message['ballot_num']
                    curr_leader = message['client_id']
                    print("Received ACCEPT", bal, "from SERVER", message['promiser'])
                    # print(f"{curr_leader} is the leader")
                elif message_type == "propose_query":
                    send_id1, send_id2 = get_other_server_ids()
                    context_id = message['context_id']
                    query_from = message['query_from']
                    query = message['query']
                    bal = message['ballot_num']
                    if bal >= ballot_num:
                        ballot_num = bal
                    print(f"Received PROPOSE QUERY for context id = {context_id} with query = {query} from {query_from}")
                    if bal >= ballot_num:
                        client_socket.send((json.dumps({
                            "type": "decide_query",
                            "context_id": context_id,
                            "query_from": query_from,
                            "query": query,
                            "send_id1": send_id1,
                            "send_id2": send_id2,
                            "client_id": client_id,
                            "ballot_num": ballot_num,
                            "kvs": key_value_store
                        }) + '\n').encode())
                        print(f"Sending DECIDE QUERY for context id = {context_id} with query = {query} to ALL")
                    else:
                        client_socket.send((json.dumps({
                                "type": "update",
                                "context_id": context_id,
                                "send_id1": send_id1,
                                "client_id": client_id,
                                "ballot_num": ballot_num,
                                "kvs": key_value_store,
                                "curr_leader": curr_leader
                            }) + '\n').encode())
                elif message_type == "propose_choose":
                    send_id1, send_id2 = get_other_server_ids()
                    context_id = message['context_id']
                    query_from = message['query_from']
                    LLM_answer = message['LLM_answer']
                    bal = message['ballot_num']
                    if bal >= ballot_num:
                        ballot_num = bal
                    print(f"Received PROPOSE CHOOSE for context id = {context_id} with answer = {LLM_answer} from {query_from}")
                    if bal >= ballot_num:
                        client_socket.send((json.dumps({
                            "type": "decide_choose",
                            "context_id": context_id,
                            "query_from": query_from,
                            "LLM_answer": LLM_answer,
                            "send_id1": send_id1,
                            "send_id2": send_id2,
                            "client_id": client_id,
                            "ballot_num": ballot_num,
                            "kvs": key_value_store
                        }) + '\n').encode())
                        print(f"Sending DECIDE CHOOSE for context id = {context_id} with answer = {LLM_answer} to ALL")
                    else:
                        client_socket.send((json.dumps({
                                "type": "update",
                                "context_id": context_id,
                                "send_id1": send_id1,
                                "client_id": client_id,
                                "ballot_num": ballot_num,
                                "kvs": key_value_store,
                                "curr_leader": curr_leader
                            }) + '\n').encode())
                elif message_type == "propose_create":
                    bal = message['ballot_num']
                    if bal[2] == ballot_num[2] + 1:
                        ballot_num = bal
                    send_id1, send_id2 = get_other_server_ids()
                    context_id = message['context_id']
                    print(f"Received PROPOSE CREATE for context id = {context_id}")
                    # print("DECIDE COUNT =", decide_count)
                    if bal >= ballot_num:
                        client_socket.send((json.dumps({
                            "type": "decide_create",
                            "context_id": context_id,
                            "send_id1": send_id1,
                            "send_id2": send_id2,
                            "client_id": client_id,
                            "ballot_num": ballot_num,
                            "kvs": key_value_store
                        }) + '\n').encode())
                        print(f"Sending DECIDE CREATE for context id = {context_id} to ALL")
                    else:
                        client_socket.send((json.dumps({
                                "type": "update",
                                "context_id": context_id,
                                "send_id1": send_id1,
                                "client_id": client_id,
                                "ballot_num": ballot_num,
                                "kvs": key_value_store,
                                "curr_leader": curr_leader
                            }) + '\n').encode())
                elif message_type == "decide_query":
                    bal = message['ballot_num']
                    decide_count[tuple(bal)] = decide_count[tuple(bal)] + 1
                    q_mes = message['query']
                    q_from = message['query_from']
                    with consensus_flag_lock:
                        # print("DECIDE COUNT =", decide_count)
                        if curr_leader == client_id and decide_count[tuple(ballot_num)] == 2:
                                leader_queue.get() #Remove from leader queue now that query is decided
                                consensus_flag = True
                                print("Received DECIDE", bal, "for query", q_mes, "from", q_from)
                    context_id = message['context_id']
                    query_from = message['query_from']
                    query = message['query']
                    with key_value_store_lock and gemini_answers_lock:
                        if decide_count[tuple(bal)] == 2:
                            key_value_store[context_id] += f"QUERY: {query}\n"
                            ask_gemini(key_value_store[context_id], context_id, query_from)
                            # print(f"You just added for context id  = {context_id} a query = {query} to key value store!!")
                elif message_type == "decide_choose":
                    bal = message['ballot_num']
                    decide_count[tuple(bal)] = decide_count[tuple(bal)] + 1
                    with consensus_flag_lock:
                        if curr_leader == client_id and decide_count[tuple(ballot_num)] == 2:
                                leader_queue.get() #Remove from leader queue now that query is decided
                                consensus_flag = True
                    context_id = message['context_id']
                    LLM_answer = message['LLM_answer']
                    query_from = message['query_from']
                    len_LLM_answer = len(LLM_answer)
                    mes_cli = message['client_id']
                    print(f"Received DECIDE {ballot_num} answer {mes_cli} {LLM_answer} from Server {query_from}")
                    with key_value_store_lock:
                        if decide_count[tuple(bal)] == 2:
                            if len(LLM_answer) >= 7 and LLM_answer[:7] == "ANSWER:":
                                key_value_store[context_id] += f"{LLM_answer}\n"
                            else:
                                key_value_store[context_id] += f"ANSWER: {LLM_answer}\n"
                            gemini_answers[context_id] = []
                            # print(f"You just added answer to key value store!! {LLM_answer}")
                elif message_type == "decide_create":
                    bal = message['ballot_num']
                    decide_count[tuple(bal)] = decide_count[tuple(bal)] + 1
                    sender = message['client_id']
                    with consensus_flag_lock:
                        if curr_leader == client_id and decide_count[tuple(ballot_num)] == 2:
                                leader_queue.get() #Remove from leader queue now that query is decided
                                consensus_flag = True
                    context_id = message['context_id']
                    with key_value_store_lock:
                        if decide_count[tuple(bal)] == 2:
                            key_value_store[context_id] = ""
                            print(f"Received DECIDE {ballot_num} create {context_id} from Server {sender}")
                elif message_type == "GEMINI":
                    context_id = message['context_id']
                    response = message['response']
                    with gemini_answers_lock:
                        gemini_answers[context_id].append(response)
                        print(f"Answer{len(gemini_answers[context_id])} for context {context_id} = {response}")
                elif message_type == "forward_to_leader":
                    message["type"] = "ack_leader_queued"
                    query_from = message["query_from"]
                    LLM_answer = message["query"]
                    message["kvs"] = key_value_store
                    client_socket.send((json.dumps(message) + '\n').encode())
                    operation = build_operation(message)
                    # print(f"okay i built the operation {operation} ")
                    threading.Thread(target=create_query_choose_context, args=(operation, query_from, LLM_answer)).start()
                elif message_type == "ack_leader_queued":
                    timeout_flag_query = False
                    input_type = message['input_type']
                    query_from = message["query_from"]
                    context_id = message["context_id"]
                    query = message["query"]
                    if input_type == "query":
                        delete_from_operation_queue(context_id, query, query_from)
                    if input_type == "create":
                        delete_from_operation_queue("create", context_id, "create")
                    if input_type == "choose":
                        delete_from_operation_queue(context_id, query, query_from)
                elif message_type == "inherit_kvs":
                    return_user = message['client_id']
                    if curr_leader == client_id:
                        # print("PUT IN LEADER QUEUE", return_user)
                        leader_queue.put((ran_mess, return_user, 'Null'))
                elif message_type == "ack_inherit_kvs":
                    return_user = message['client_id']
                    key_value_store = message['kvs']
                    ballot_num = message['ballot_num']
                    curr_leader = message['curr_leader']
                    # print(f"Updated key value store {key_value_store}")
                    client_socket.send((json.dumps({
                        "type": "ack_ack_inherit_kvs",
                        "return_user": return_user,
                        "ballot_num": ballot_num,
                        "kvs": key_value_store
                    })+'\n').encode())
                elif message_type == "ack_ack_inherit_kvs":
                    leader_queue.get()
                    consensus_flag = True
            else:
                print(f"Received non-JSON message: {message}")
                handle_exit()

def timed_out(type):
    global timeout_flag_query, timeout_flag_proposal
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
    global leader_queue, operation_queue, ran_mess, stop_elect_flag
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
        input1 = parts[1]
        input2 = " ".join(parts[2:])
        input3 = query_from
        with key_value_store_lock:
            if input1 not in key_value_store:
                print(f"Could not process query = {input2} for context id = {input1} because that context id has not been created yet!")
                return
        with gemini_answers_lock:
            if len(gemini_answers[input1]) > 0:
                print(f"Sorry we can not process another query for context id = {input1} until an answer has been chosen for the previous query!")
                return
    elif input_type == "choose":
        input1 = parts[1]
        input3 = "choose"
        with key_value_store_lock and gemini_answers_lock:
            if input1 not in key_value_store:
                print(f"Could not choose from context id = {input1} because that context id has not been create yet!")
                return
            if query_from == client_id:
                if int(parts[2]) < 1 or int(parts[2]) > len(gemini_answers[parts[1]]):
                    print(f"Could not choose from context id = {input1} for answer = {parts[2]} because that is not a valid answer to choose from!")
                    return
                input2 = gemini_answers[parts[1]][int(parts[2])-1]
            else:
                input2 = LLM_answer
    
    if curr_leader == 0:
        operation_queue.put((input1, input2, input3))
        ran_mess = message
        propose()
    elif curr_leader == client_id:
        ran_mess = message
        leader_queue.put((input1, input2, input3))
        # print(f"just added to leader queue ({input1} {input2} {input3})")
    else:
        ran_mess = message
        print("Putting", message, "in operation queue")
        operation_queue.put((input1, input2, input3))
        send_to_leader(message)
        threading.Timer(10, timed_out, args=("operation",)).start()
    return

def send_to_leader(message):
    parts = message.split()
    if curr_leader != 0:
        context_id = parts[1]
        query = "Null"
        answer_num = 0
        if parts[0] == "query":
            query = " ".join(parts[2:])
        elif parts[0] == "choose":
            answer_num = parts[2]
            query = gemini_answers[context_id][int(answer_num)-1]
        client_socket.send((json.dumps({
            "type": "forward_to_leader",
            "query_from": client_id,
            "curr_leader": curr_leader,
            "input_type": parts[0],
            "context_id": context_id,
            "query": query,
            "answer_num": answer_num,
            "ballot_num": ballot_num,
            "kvs": key_value_store
        }) + '\n').encode())

def view_context(message):
    _, context_id = message.split()
    with key_value_store_lock:
        print(key_value_store[context_id])

def view_all_context():
    with key_value_store_lock:
        print(key_value_store)

def consensus_operation(input1, input2, input3):
    send_id1, send_id2 = get_other_server_ids()
    print(f"Getting CONSENSUS on {input1} {input2} with {input3}")
    if input3.startswith("create"):
        context_id = input2
        ballot_num[2] += 1
        print("Sending PROPOSE CREATE", ballot_num, "to ALL")
        client_socket.send((json.dumps({
            "type": "propose_create",
            "context_id": context_id,
            "send_id1": send_id1,
            "send_id2": send_id2,
            "client_id": client_id,
            "ballot_num": ballot_num,
            "kvs": key_value_store
        })+'\n').encode())
    elif input3 == "choose":
        context_id = input1
        LLM_answer = input2
        query_from = input3
        ballot_num[2] += 1
        decide_count[tuple(ballot_num)] = 0
        client_socket.send((json.dumps({
            "type": "propose_choose",
            "context_id": context_id,
            "query_from": query_from,
            "LLM_answer": LLM_answer,
            "send_id1": send_id1,
            "send_id2": send_id2,
            "client_id": client_id,
            "ballot_num": ballot_num,
            "kvs": key_value_store
        })+'\n').encode())
    else:
        context_id = input1
        query = input2
        ballot_num[2] += 1
        decide_count[tuple(ballot_num)] = 0
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
    return

def leader_queue_thread():
    global curr_leader, leader_queue, operation_queue, consensus_flag, timeout_flag_proposal
    while True:
        with consensus_flag_lock:
            if curr_leader == client_id and not leader_queue.empty() and consensus_flag:
                print("Handling a leader operation")
                consensus_flag = False
                input1, input2, input3 = leader_queue.queue[0]
                if ballot_num[1] == client_id:
                    consensus_operation(input1, input2, input3)
                else:
                    curr_leader = ballot_num[1]
                    leader_queue = leader_queue.queue.clear()
                    timeout_flag_proposal = False

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
    # testing_purposes(True)
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