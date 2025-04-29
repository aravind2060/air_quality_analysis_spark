import socket
import time
import pandas as pd

host = 'localhost'  # Server address
port = 9999  # Port to connect to

def start_tcp_client():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        # Connect to the server
        client_socket.connect((host, port))
        print(f"Connected to server at {host}:{port}")

        # Read CSV file
        file_path = "pollution_data.csv"
        df = pd.read_csv(file_path)

        for _, row in df.iterrows():
            # Construct the message in the desired format
            timestamp = row['timestamp']
            region = row['region']
            metric = row['metric']
            value = row['value']
            message = f"{timestamp},{region},{metric},{value}"

            try:
                # Send message to the server
                client_socket.sendall(message.encode('utf-8'))
                print(f"Sent: {message}")
                time.sleep(1)  # Simulate real-time streaming

            except BrokenPipeError:
                print("Error: Broken pipe. The connection was closed by the server.")
                break
            except Exception as e:
                print(f"Error: {e}")
                break

    except Exception as e:
        print(f"Connection failed: {e}")

    finally:
        client_socket.close()
        print("Connection closed.")

if __name__ == "__main__":
    start_tcp_client()
