import socket
import threading
import sys
import time


clients = []
running = True
id_dct = {}
fail_dct = {}
def primary_handle_client(client_socket): #Everytime a client connects, it has its own thread to communicate with the server
    global running
    buffer = ""#Need this because messages were being concatenated
    while running:
        try:
            message = client_socket.recv(1024).decode() 
            buffer += message
            while '\n' in buffer:
                message, buffer = buffer.split('\n', 1)
                parts = message.split()
                print(message)
                if not message:
                    break
                #This  is if you want to forward to the other 2 servers
                if message.startswith("prepare") or message.startswith("propose_query") or message.startswith("propose_create") or message.startswith("decide_query") or message.startswith("decide_create"):
                    send_id1 = parts[-2]
                    send_id2 = parts[-3]
                    # print(f"Received ids {send_id1} {send_id2}")
                    # # print(f"clients {clients}")
                    # print(f"id dict {id_dct}")
                    # print(f"fail dct {fail_dct}")
                    if(fail_dct[id_dct[send_id1]] == True): #send prepare to both other clients
                        threading.Thread(target=server_to_client, args=(clients[(int(send_id1)-1)], message), daemon=True).start()
                    if(fail_dct[id_dct[send_id2]] == True):
                        threading.Thread(target=server_to_client, args=(clients[(int(send_id2)-1)], message), daemon=True).start()
                #This is for forwarding to 1 other server
                elif message.startswith("promise") or message.startswith("accept") or message.startswith("query") or message.startswith("ack_leader_queued") or message.startswith("GEMINI"): #send back to proposer
                    forward = parts[1]
                    if fail_dct[id_dct[forward]] == True:
                        threading.Thread(target=server_to_client, args=(clients[(int(forward)-1)], message), daemon=True).start()
        except ConnectionResetError as e:
            print(e)
            break
        except Exception as e:
            print(e)
            break
    client_socket.close() 

def server_to_client(client_socket, message): #handles send messages from server to clients
    try:
        time.sleep(3)
        client_socket.send(f"{message}\n".encode())  
        print(f"Forwarded {message}")
    except Exception as e:
        print(e)
        pass
    return



def primary_input_thread(): #primary input thread
    global running
    while running:
        command = input()
        if command.lower() == "exit":
            handle_exit()

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
            id_dct[client_id] = port_num
            fail_dct[port_num] = True
            client_socket.send("Success".encode()) 
            client_handler = threading.Thread(target=primary_handle_client, args=(client_socket,), daemon=True) #handles incoming clinets
            client_handler.start()  
            print(id_dct)
        finally:
            pass
    server_socket.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)
    start_server(int(sys.argv[1]))