import socket
import threading
import sys
import time


clients = []
running = True
id_dct = []
def primary_handle_client(client_socket): #Everytime a client connects, send all other clients the updated peer list and handles input from clients
    global running
    while running:
        try:
            message = client_socket.recv(1024).decode('utf-8') 
            if not message:
                break
            threading.Thread(target=client_to_primary, args=(client_socket, message), daemon=True).start()
        except ConnectionResetError:
            break
        except Exception as e:
            break
    client_socket.close() 

def client_to_primary(client_socket, message): #handles client message in primary
    try:
        response = ""
        time.sleep(3)
        parts = message.split()
        print("The message received from client is", parts)
        response = "Success"
        client_socket.send(f"{response}".encode('utf-8'))  
    except Exception as e:
        pass



def primary_input_thread(): #primary input thread
    global running
    while running:
        command = input()
        if command.lower() == "dictionary":
            print("{", end ="")
            i = 0
            for key in sorted(ht):
                if i != 0:
                    print(", ", end ="")
                print(f"({key}, {ht[key]})", end = "")
                i += 1
            print("}")
        elif command.lower() == "exit":
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


def start_server(PORT): #Begines the input thread and accepts clients
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
            clients.append(client_socket)  
            client_id = client_socket.recv(1024).decode('utf-8')
            id_dct.append(client_id)
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