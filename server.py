import socket
import threading
import sys
import time
import ast
import json

clients = []
sockets = {}
id_sockets = {}
running = True
id_dct = {}
fail_dct = {}
fail_links = []
def is_json(message):
    try:
        # Attempt to parse the string as JSON
        json_object = json.loads(message)
        return True, json_object  # It is JSON and return the parsed object
    except json.JSONDecodeError:
        return False, message  # It is not JSON, return the original string
    
def primary_handle_client(client_socket): #Everytime a client connects, it has its own thread to communicate with the server
    global running
    buffer = "" #Need this because messages were being concatenated
    while running:
        try:
            message = client_socket.recv(1024).decode() 
            buffer += message
            # print(f"IS THIS JSON {message}")
            # print(f"should i convert {message["type"]}")
            while '\n' in buffer:
                message, buffer = buffer.split('\n', 1)
                print(f"received {message}")
                bool_json, message = is_json(message)
                if not bool_json:
                    parts = message.split()
                    if not message:
                        break
                    #This  is if you want to forward to the other 2 servers
                    if message.startswith("prepare") or message.startswith("propose_query") or message.startswith("propose_choose") or message.startswith("propose_create") or message.startswith("decide_query") or message.startswith("decide_choose") or message.startswith("decide_create"):
                        send_id1 = parts[-2]
                        send_id2 = parts[-3]

                        print("Failed links", fail_links)
                        print("Current socket", sockets[client_socket])

                        if(fail_dct[id_dct[send_id1]] == True and (sockets[client_socket],send_id1) not in fail_links):
                         #send prepare to both other clients
                            print("Sending from", sockets[client_socket], "to", send_id1)

                            threading.Thread(target=server_to_client, args=(clients[(int(send_id1)-1)], message, "string"), daemon=True).start()
                        if(fail_dct[id_dct[send_id2]] == True and (sockets[client_socket],send_id2) not in fail_links): #send prepare to both other clients
                            print("Sending from", sockets[client_socket], "to", send_id1)

                            threading.Thread(target=server_to_client, args=(clients[(int(send_id2)-1)], message, "string"), daemon=True).start()
                    #This is for forwarding to 1 other server
                    elif message.startswith("promise") or message.startswith("accept") or message.startswith("query") or message.startswith("ack_leader_queued") or message.startswith("GEMINI"): #send back to proposer
                        forward = parts[1]
                        if fail_dct[id_dct[forward]] == True and (sockets[client_socket],forward) not in fail_links:
                            threading.Thread(target=server_to_client, args=(clients[(int(forward)-1)], message, "string"), daemon=True).start()
                else: #Process it as json string
                    message_type = message['type']
                    print(f"Found a json string with type {message_type}!!!")
                    if message_type == "forward_to_leader" or message_type == "ack_leader_queued":
                        forward = message["curr_leader"]
                        print(f"Forwarding message = {message} to leader = {forward}")
                        if fail_dct[id_dct[str(forward)]] == True and (sockets[client_socket],str(forward)) not in fail_links:
                            threading.Thread(target=server_to_client, args=(clients[(int(forward)-1)], message, "json"), daemon=True).start()
        except ConnectionResetError as e:
            print(e)
            break
        except Exception as e:
            print(e)
            break
    client_socket.close() 

def server_to_client(client_socket, message, type): #handles send messages from server to clients
    time.sleep(3)
    if type == "string":

        try:
            client_socket.send(f"{message}\n".encode())  
            print(f"Forwarded {message}")
        except Exception as e:
            print(e)
    elif type == "json":
        try:
            client_socket.send((json.dumps(message)+'\n').encode())  
            print(f"Forwarded {message}")
        except Exception as e:
            print(e)
    return

def handle_fail_link(command): #handles fail link
    global fail_links

    message = command.split()
    src = message[1]
    dest = message[2]

    fail_links.append((src,dest))
    fail_links.append((dest,src))
    print("Failed Link between", src, "and", dest)
    return


def handle_fix_link(command): #handles fail link
    global fail_links

    message = command.split()
    src = message[1]
    dest = message[2]

    fail_links.pop((src, dest))
    fail_links.pop((dest, src))
    print("Fixed Link between", src, "and", dest)
    return

def handle_fail_node(command):
    global fail_dct

    message = command.split()
    node = message[1]

    fail_dct[node] = False #may need to check the dictionary id is correct

    if node in id_sockets:
        try:
            id_sockets[node].close()
            del id_sockets[node]  # Remove the socket from the dictionary
        except Exception as e:
            print(e)
            pass
    return


def primary_input_thread(): #primary input thread
    global running
    while running:
        command = input()
        print(f"Command = {command.lower()}")
        if command.lower() == "exit":
            handle_exit()
        elif command.lower().startswith("faillink"):
            handle_fail_link(command)
        elif command.lower().startswith("fixlink"):
            handle_fix_link(command)
        elif command.lower().startswith("failNode"):
            handle_fail_node(command)

def handle_exit(): #handles exits
    global running
    running = False
    for sock in clients:
        try:
            sock.close()
        except Exception as e:
            print(e)
            pass
    clients.clear()  
    sys.stdout.flush()
    sys.exit(0)


def start_server(PORT): #Begins the input thread and accepts clients
    global server_socket 
    global running
    global sockets
    global id_socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(('127.0.0.1', int(PORT))) 
    server_socket.listen(10)  
    threading.Thread(target=primary_input_thread, daemon=True).start()
    while running:
        try:
            client_socket, _ = server_socket.accept()
            clients.append(client_socket) #This only tracks order if clients are accepted as 1,2,3
            client_id, port_num = client_socket.recv(1024).decode().split()

            sockets[client_socket] = client_id  #dictionary of sockets
            id_sockets[client_id] = client_socket

            id_dct[client_id] = port_num
            fail_dct[port_num] = True
            client_socket.send("Success".encode()) 
            client_handler = threading.Thread(target=primary_handle_client, args=(client_socket,), daemon=True) #handles incoming clinets
            client_handler.start()  
            print(id_dct)
        except Exception as e:
            print(e)
            pass
    server_socket.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)
    start_server(int(sys.argv[1]))