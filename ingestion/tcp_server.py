import socket
import time

HOST = 'localhost'
PORT = 9999

# Start TCP Server
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((HOST, PORT))
server_socket.listen(1)

print(f"Listening for connections on {HOST}:{PORT}")

conn, addr = server_socket.accept()
print(f"Connected by {addr}")

# Sample data to send
sample_data = [
    "8118,New Delhi,India,2025-01-01 01:00:00,pm25,122.0,µg/m³,28.63576,77.22445",
    "8120,Los Angeles,USA,2025-01-01 01:00:00,pm10,80.5,µg/m³,34.05223,-118.24368",
    "8121,Paris,France,2025-01-01 01:00:00,no2,50.2,µg/m³,48.85661,2.35222"
]

for record in sample_data:
    message = record + "\n"
    conn.sendall(message.encode('utf-8'))
    print(f"Sent: {record}")
    time.sleep(2)

conn.close()
server_socket.close()