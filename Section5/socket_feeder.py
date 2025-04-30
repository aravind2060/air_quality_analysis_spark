#!/usr/bin/env python3
"""
A simple TCP socket feeder for testing Section5 pipeline.
Writes a few sample air‐quality lines to localhost:9999.
"""

import socket
import time

HOST, PORT = '127.0.0.1', 9999

# Sample lines—feel free to adjust timestamps & values
lines = [
    "[loc1, sensor42, SomePlace, 2025-04-29T12:00:00+00:00, 40.7, -74.0, pm25, µg/m3, 12.3]",
    "[loc1, sensor42, SomePlace, 2025-04-29T12:00:05+00:00, 40.7, -74.0, temperature, C, 22.1]",
    "[loc1, sensor42, SomePlace, 2025-04-29T12:00:10+00:00, 40.7, -74.0, humidity, %, 55.0]",
]

def main():
    # Create a listening socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST, PORT))
        server.listen(1)
        print(f"Feeder listening on {HOST}:{PORT} ...")
        conn, addr = server.accept()
        print(f"Client connected: {addr}")
        with conn:
            for line in lines:
                msg = line + "\n"
                conn.sendall(msg.encode("utf-8"))
                print(f"Sent: {line}")
                time.sleep(1)
            print("Seed data sent. Keeping connection open...")
            while True:
                time.sleep(10)  # keep socket alive

if __name__ == "__main__":
    main()
