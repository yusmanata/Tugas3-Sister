import asyncio
import json
import socket
import time
from locust import User, task, between, events

class QueueTCPClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def send_request(self, name, message):
        start_time = time.time()
        try:
            # Locust tasks run in threads, so we use synchronous sockets for simplicity here
            # or we could use an async event loop, but Locust's standard User is sync-friendly
            with socket.create_connection((self.host, self.port), timeout=2) as sock:
                # Our protocol: JSON string encoded as bytes
                data = json.dumps(message).encode()
                sock.sendall(data)
                
                # If we expect a response, we'd read it here. 
                # For 'produce', our current QueueNode doesn't send a TCP response back 
                # (it just processes it). 
                
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="TCP",
                name=name,
                response_time=total_time,
                response_length=len(data),
            )
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="TCP",
                name=name,
                response_time=total_time,
                exception=e,
            )

class QueueUser(User):
    wait_time = between(0.1, 0.5) # Simulate users sending messages every 100-500ms

    def on_start(self):
        # We target Node 1 as the entry point
        self.client = QueueTCPClient("localhost", 8001)

    @task
    def produce_message(self):
        # Format for a raw PRODUCE message expected by QueueNode
        # Note: In a real run, you'd need node_1 to be running
        message = {
            "type": "PRODUCE",
            "topic": "load_test_topic",
            "payload": "High load test message content",
            "sender_id": "locust_client"
        }
        self.client.send_request("ProduceMessage", message)
