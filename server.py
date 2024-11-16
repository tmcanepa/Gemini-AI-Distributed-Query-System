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
    while running:
        try:
            message = client_socket.recv(1024).decode('utf-8') 
            if not message:
                break
            if message.startswith("prepare"):
                parts = message.split()
                send_id1 = parts[3]
                send_id2 = parts[4]
                print(f"Received prepare")
                if(fail_dct[id_dct[send_id1]] == True): #send to both other clients
                    threading.Thread(target=server_to_client, args=(clients[send_id1-1], message), daemon=True).start()
                if(fail_dct[id_dct[send_id2]] == True):
                    threading.Thread(target=server_to_client, args=(clients[send_id2-1], message), daemon=True).start()
            elif message.startswith("promise"): #send back to proposer
                _, _, _, _, proposer, promiser = message.split()
                if fail_dct[id_dct[proposer]] == True:
                    threading.Thread(target=server_to_client, args=(clients[proposer-1], message), daemon=True).start()
            elif message.startswith("accept"): #send back to promiser
                _, b, promiser, acceptor = message.split()
                if fail_dct[id_dct[promiser]] == True:
                    threading.Thread(target=server_to_client, args=(clients[promiser-1], message), daemon=True).start()
            elif message.startswith("accepted"): #send back to promiser
                _, b,  acceptor, acceptedor = message.split()
                if fail_dct[id_dct[promiser]] == True:
                    threading.Thread(target=server_to_client, args=(clients[promiser-1], message), daemon=True).start()
            
            elif message.startswith(create)
                
        except ConnectionResetError:
            break
        except Exception as e:
            break
    client_socket.close() 

def server_to_client(client_socket, message): #handles send messages from server to clients
    try:
        time.sleep(3)
        client_socket.send(message.encode('utf-8'))  
    except Exception as e:
        pass



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
            client_id, port_num = client_socket.recv(1024).decode('utf-8').split()
            id_dct[client_id] = port_num
            fail_dct[port_num] = True
            client_socket.send("Success".encode('utf-8')) 
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