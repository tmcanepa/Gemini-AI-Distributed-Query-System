# GEMINI Distributed Chat System

## Features
- Gemini Chat Agent that gets multiple responses and allows you to select your favorite
- Create new chats that save your responses
- Implements a Multi-Paxos consensus system
- Allows for mock network failures and client crashes

## Prerequisites

To test or use this project, ensure you have the following installed:

- [Python 3.12.1 or newer](https://www.python.org/downloads/)  
- Any required packages listed in `requirements.txt` (e.g., `gemini`, `socket`).  
- A Gemini API [KEY](https://ai.google.dev/gemini-api/docs/api-key)  

## Setup

1. Clone this repository:
   ```bash
   git clone https://github.com/tmcanepa/Gemini-AI-Distributed-Query-System.git
   cd Gemini-AI-Distributed-Query-System
   ```

2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure your .env file:
  There should be a .env.example with how your .env file should look.  
  a. Copy the `.env.example` into a new `.env` file  
  b. Add your Gemini API key to the `.env` file  


4. Ensure all files are in the correct directory structure as specified in the project.

## How to Use

### Terminal Setup
This project requires multiple terminals to run different components. Follow these steps:  
Start by entering the project portion of the code:  
```bash
   cd project
```

#### Terminal 1: Start the Network Connection Server
Start the network linkage server to allow for message passing between clients:  
Select any desired PORT number but keep in consistent across both server and clients  
WINDOWS  
```bash
python server.py [$(PORT)]
```
MAC/LINUX  
```bash
make server
```

This terminal will display logs and messages related to the server's actions and client requests.
The server also allows fo you to input commands where you can simmulate client failures/crashes, network failures, and network partitions. For Example:
- `failNode <number>`: Terminates the client with the associated number which will act as though the client had crashed. Multiple clients can be failed.
- `failLink <client number> <client number>`: Fails the network link that connects the first and second client specified. This prevents messages from being sent in either direction between the 2 clients
- `fixLink <client number> <client number>`: Fixes the network link that connects the first and second client specified. This reestablishes the connection that allows messages from being sent between the 2 clients 
- `exit`: Closes the server and stops all of its running processes


#### Terminal 2: Start Client 1
Run the first client to start sending requests:
WINDOWS
```bash
python client.py 1 [$(PORT)]
```
MAC/LINUX
```bash
make client1
```
This terminal will allow you to input commands for Client 1. For example:
- `create <number>`: Creates a CONTEXT_ID associated the number provided. This acts simmilarly to creating a new chat with gemini where you can insert new queries seperate from any other context.
- `query <CONTEXT_ID> <query>`: Queries gemini on the CONTEXT_ID with the query provided and returns the response from each individual client where you can select the answer that you like best. 
- `choose <CONTEXT_ID> <answer number>`: Chooses the response you would like to save in the designated context.
- `view <CONTEXT_ID>`: Display the current state of the "chat" at the specified context.
- `viewall`: Show all queried "chat" entries for each context.
- `exit`: Closes the current client and stops all of its running processes

#### Terminal 3: Start Client 2
Run the second client to send requests:
```bash
python client.py 2 [$(PORT)]
```
MAC/LINUX
```bash
make client2
```
Similar to Client 1, this terminal accepts commands like `create`, `query`, `choose`, `view`, and `viewall`.


#### Terminal 4: Start Client 3
Run the second client to send requests:
```bash
python client.py 3 [$(PORT)]
```
MAC/LINUX
```bash
make client3
```
Similar to Client 1 and 2, this terminal accepts commands like `create`, `query`, `choose`, `view`, and 

### Testing the Project

1. Start all servers and clients as described above.
2. Use the client terminals to issue commands and observe how the servers handle requests.
3. Commands can be run concurrently as the system uses Multi-Paxos to reach consensus and stores the same information across all clients. 
4. Monitor the server terminals to verify load balancing and replication functionality.

## Contributors
- [Tyler Canepa](https://github.com/tmcanepa)
- [Kai Maeda](https://github.com/kai-maeda)

