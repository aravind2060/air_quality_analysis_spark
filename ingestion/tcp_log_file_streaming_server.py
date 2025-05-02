import socket
import time

# File containing your pollution data
DATA_FILE = "pollution_data.csv"

# Host and Port Configuration
HOST = 'localhost'  # or '127.0.0.1'
PORT = 9999         # Make sure this matches your Spark code too!

# Create the socket
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((HOST, PORT))
server_socket.listen(1)

print(f"Server started at {HOST}:{PORT}. Waiting for connection...")

# Accept one client (Spark will connect)
client_socket, addr = server_socket.accept()
print(f"Client connected from: {addr}")

try:
    with open(DATA_FILE, "r") as file:
        # Send each line from the CSV file to the client
        for line in file:
            print(f"Sending: {line.strip()}")
            try:
                client_socket.send((line.strip() + "\n").encode('utf-8'))
                time.sleep(1)  # Simulate real-time data, 1 second delay
            except BrokenPipeError:
                print("Error: Broken pipe. The connection was closed by the client.")
                break  # Stop sending if the connection is closed

except Exception as e:
    print(f"Error reading file: {e}")

finally:
    client_socket.close()
    server_socket.close()
    print("Server closed.")
