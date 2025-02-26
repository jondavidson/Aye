#!/usr/bin/env python3
import socket
import struct
import pyarrow as pa
import pyarrow.ipc as ipc

# --- Parameter Defaults ---
KDB_HOST = "localhost"
KDB_PORT = 5001
KDB_COMMAND = "getArrowData"  # Command to request Arrow data from kdb+
BUFFER_SIZE = 16384  # Size (in bytes) for each socket.recv() call

def fetch_arrow_table(host=KDB_HOST, port=KDB_PORT, command=KDB_COMMAND, buffer_size=BUFFER_SIZE):
    """
    Connects to the kdb+ server, sends the command, reads the 8-byte header to determine the payload length,
    and then reads the full Arrow IPC stream. Returns a PyArrow Table.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        # Send command encoded in Latin-1 (compatible with ASCII)
        s.sendall(command.encode("latin1") + b"\n")

        # Read the 8-byte header to determine total length of the Arrow IPC payload.
        header = b""
        while len(header) < 8:
            chunk = s.recv(8 - len(header))
            if not chunk:
                raise ConnectionError("Socket closed before header received")
            header += chunk
        total_length = struct.unpack("!Q", header)[0]  # Network byte order (big-endian)

        # Read the remaining data based on the header’s total_length.
        data = bytearray()
        bytes_remaining = total_length
        while bytes_remaining > 0:
            recv_size = min(buffer_size, bytes_remaining)
            chunk = s.recv(recv_size)
            if not chunk:
                raise ConnectionError("Socket closed before full data received")
            data.extend(chunk)
            bytes_remaining -= len(chunk)

    # Use a BufferReader to allow PyArrow's RecordBatchStreamReader to parse the stream.
    reader = ipc.RecordBatchStreamReader(pa.BufferReader(bytes(data)))
    arrow_table = reader.read_all()
    return arrow_table

if __name__ == "__main__":
    try:
        table = fetch_arrow_table()
        print("Arrow Table Received:")
        print(table)
    except Exception as e:
        print(f"Error fetching Arrow table: {e}")
