import socket
import threading
import sys
import time
import json
import os

clients = [None] * 3
sockets = {}
id_sockets = {}
running = True
id_dct = {}
fail_dct = {}
fail_links = []

def is_json(message):
    try:
        json_object = json.loads(message)
        return True, json_object
    except json.JSONDecodeError:
        return False, message

def primary_handle_client(client_socket): # Everytime a client connects, it has its own thread to communicate with the server
    global running
    buffer = "" # Need this because messages were being concatenated
    while running:
        try:
            message = client_socket.recv(1024).decode()
            if not message:
                print("Socket has been shutdown. Exiting.")
                break
            buffer += message
            while '\n' in buffer:
                message, buffer = buffer.split('\n', 1)
                bool_json, message = is_json(message)
                print(f"received {message['type']}")
                if bool_json:
                    message_type = message['type']
                    # print(f"Found a json string with type {message_type}!!!")
                    if message_type in ["prepare", "propose_query", "propose_choose", "propose_create", "decide_query", "decide_choose", "decide_create", "inherit_kvs"]:
                        send_id1 = message['send_id1']
                        send_id2 = message['send_id2']
                        # print("Failed links", fail_links)
                        # print("Current socket", sockets[client_socket])
                        if send_id1 in fail_dct and fail_dct[send_id1] and (sockets[client_socket], send_id1) not in fail_links:
                            # print("Sending from", sockets[client_socket], "to", send_id1)
                            threading.Thread(target=server_to_client, args=(clients[int(send_id1) - 1], message), daemon=True).start()
                        if send_id2 in fail_dct and fail_dct[send_id2] and (sockets[client_socket], send_id2) not in fail_links:
                            # print("Sending from", sockets[client_socket], "to", send_id2)
                            threading.Thread(target=server_to_client, args=(clients[int(send_id2) - 1], message), daemon=True).start()
                        if fail_dct[sockets[client_socket]] and (sockets[client_socket], sockets[client_socket]) not in fail_links and message_type not in ["prepare","inherit_kvs"]:
                            # print("Sending from", sockets[client_socket], "to", sockets[client_socket])
                            threading.Thread(target=server_to_client, args=(clients[int(sockets[client_socket]) - 1], message), daemon=True).start()
                    elif message_type in ["promise", "accept", "query", "ack_leader_queued", "GEMINI"]:
                        if message_type in ["GEMINI"]:
                            forward = message['query_from']
                        elif message_type == "promise":
                            forward = message["proposer"]
                        elif message_type == "accept":
                            forward = message["promiser"]
                        else:
                            forward = message['client_id']
                        # print("Forwardng", message_type, "to", forward)
                        if forward in fail_dct and fail_dct[forward] and (sockets[client_socket], forward) not in fail_links:
                            threading.Thread(target=server_to_client, args=(clients[int(forward) - 1], message), daemon=True).start()
                    elif message_type == "forward_to_leader":
                        forward = message["curr_leader"]
                        if forward in fail_dct and fail_dct[forward] and (sockets[client_socket], forward) not in fail_links:
                            threading.Thread(target=server_to_client, args=(clients[int(forward) - 1], message), daemon=True).start()
                    elif message_type == "ack_leader_queued":
                        print(f"message = {message}")
                        forward = message["client_id"]
                        if forward in fail_dct and fail_dct[forward] and (sockets[client_socket], forward) not in fail_links:
                            threading.Thread(target=server_to_client, args=(clients[int(forward) - 1], message), daemon=True).start()
                    elif message_type in ["ack_inherit_kvs", "ack_ack_inherit_kvs"]:
                        forward = message["return_user"]
                        if fail_dct[forward] and (sockets[client_socket], forward) not in fail_links:
                            threading.Thread(target=server_to_client, args=(clients[int(forward) - 1], message), daemon=True).start()
                else:
                    print(f"Received non-JSON message: {message}")
        except ConnectionResetError as e:
            print(e)
            break
        except Exception as e:
            print(e)
            break
    client_socket.close()

def server_to_client(client_socket, message): # handles send messages from server to clients
    time.sleep(3)
    try:
        client_socket.send((json.dumps(message) + '\n').encode())
        print(f"Forwarded {message}")
    except Exception as e:
        print(e)
    return

def handle_fail_link(command): # handles fail link
    global fail_links

    message = command.split()
    src = int(message[1])
    dest = int(message[2])

    fail_links.append((src, dest))
    fail_links.append((dest, src))
    print("Failed Link between", src, "and", dest)
    return

def handle_fix_link(command): # handles fail link
    global fail_links

    message = command.split()
    src = int(message[1])
    dest = int(message[2])

    fail_links.remove((src, dest))
    fail_links.remove((dest, src))
    print("Fixed Link between", src, "and", dest)
    return

def handle_fail_node(command):
    global fail_dct

    parts = command.split()
    node = int(parts[1])

    fail_dct[node] = False 
    print(f"We just failed node = {node}, fail_dct = {fail_dct}")

    if node in id_sockets and id_sockets[node]:
        try:
            id_sockets[node].shutdown(socket.SHUT_RDWR)
            id_sockets[node].close()
        except Exception as e:
            print(f"Error closing socket for node {node}: {e}")
        del id_sockets[node]
    return

def primary_input_thread(): # primary input thread
    global running
    while running:
        command = input()
        # print(f"Command = {command.lower()}")
        if command.lower() == "exit":
            handle_exit()
        elif command.lower().startswith("faillink"):
            handle_fail_link(command)
        elif command.lower().startswith("fixlink"):
            handle_fix_link(command)
        elif command.lower().startswith("failnode"):
            handle_fail_node(command)
        else:
            print(f"{command} is not a valid input...")
            print("1. failLink <src> <dst>")
            print("2. fixLink <src> <dst>>")
            print("3. failNode <nodeNum>")
            print("4. exit")

def handle_exit(): # handles exits
    global running
    running = False
    for sock in clients:
        try:
            sock.close()
        except Exception as e:
            # print(e)
            pass  
    try:
        clients.clear()
        sys.stdout.flush()
        os._exit(0)
    except Exception as e:
        pass

def start_server(PORT): # Begins the input thread and accepts clients
    global server_socket,running,sockets,id_sockets

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(('127.0.0.1', int(PORT)))
    server_socket.listen(10)
    threading.Thread(target=primary_input_thread, daemon=True).start()
    while running:
        try:
            client_socket, _ = server_socket.accept()
            response = client_socket.recv(1024).decode()
            _ , response_json = is_json(response)
            client_id = response_json["client_id"]
            # port_num = response_json["port_num"]


            clients[client_id - 1] = client_socket # This only tracks order if clients are accepted as 1,2,3
            sockets[client_socket] = client_id  # dictionary of sockets
            id_sockets[client_id] = client_socket
            fail_dct[client_id] = True

            client_socket.send((json.dumps({"type": "Success"}) + '\n').encode())

            client_handler = threading.Thread(target=primary_handle_client, args=(client_socket,), daemon=True) # handles incoming clients
            client_handler.start()
            # print(fail_dct)
            # print(f"length of id sockets = {len(id_sockets)}")
        except Exception as e:
            print(e)
            pass
    server_socket.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)
    start_server(int(sys.argv[1]))