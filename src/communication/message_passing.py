import asyncio
import json
import logging
from typing import Callable, Any, Coroutine, Dict
from src.utils.config import NETWORK_BUFFER_SIZE
from src.utils.metrics import metrics
import base64
try:
    from src.security.crypto import SecurityCrypto
except ImportError:
    SecurityCrypto = None

class MessagePasser:
    def __init__(self, node_id: str, host: str, port: int):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.server = None
        self.message_handler: Callable[[Dict[str, Any]], Coroutine] = None
        self.peers = {} # node_id -> (host, port)
        
        # Partition simulator: set of node_ids that this node cannot communicate with
        self.partitioned_nodes = set()
        self.logger = logging.getLogger(node_id)

    def set_peers(self, cluster_config: dict):
        for nid, address in cluster_config.items():
            if nid != self.node_id:
                self.peers[nid] = address

    def register_handler(self, handler: Callable[[Dict[str, Any]], Coroutine]):
        self.message_handler = handler

    async def start_server(self):
        # Always bind to 0.0.0.0 in Docker/distributed environments to listen on all interfaces
        self.server = await asyncio.start_server(
            self._handle_client, '0.0.0.0', self.port
        )
        self.logger.info(f"Server listening on {self.host}:{self.port}")
        
    async def stop_server(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()

    async def _handle_client(self, reader, writer):
        try:
            data = await reader.read(NETWORK_BUFFER_SIZE)
            if data:
                raw_payload = json.loads(data.decode())
                
                # Intercept for E2E Encryption
                if SecurityCrypto and "encrypted_payload" in raw_payload:
                    ciphertext = base64.b64decode(raw_payload["encrypted_payload"])
                    message = SecurityCrypto.decrypt_payload(ciphertext)
                    
                    # Verify Certificate
                    token = message.get("auth_token")
                    sender_id = message.get("sender_id")
                    if not SecurityCrypto.verify_node_certificate(token, sender_id):
                        self.logger.warning(f"Security Alert: Invalid node certificate from {sender_id}")
                        writer.close()
                        return
                else:
                    message = raw_payload
                    
                sender_id = message.get('sender_id')
                
                # Check for network partition (incoming)
                if sender_id in self.partitioned_nodes:
                    self.logger.debug(f"Partition active. Dropped incoming message from {sender_id}")
                    writer.close()
                    return

                metrics.inc_received()
                if self.message_handler:
                    # Run handler in background
                    asyncio.create_task(self.message_handler(message))
                    
        except Exception as e:
            self.logger.error(f"Error receiving message: {e}")
        finally:
            writer.close()

    async def send_message(self, target_node_id: str, message: dict):
        # Check for network partition (outgoing)
        if target_node_id in self.partitioned_nodes:
            self.logger.debug(f"Partition active. Cannot send to {target_node_id}")
            return False

        if target_node_id not in self.peers:
            self.logger.error(f"Unknown peer: {target_node_id}")
            return False

        host, port = self.peers[target_node_id]
        message['sender_id'] = self.node_id
        
        try:
            # We use a short timeout for connecting so Raft doesn't block forever
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), 
                timeout=0.5
            )
            
            # Intercept for E2E Encryption
            if SecurityCrypto:
                message["auth_token"] = SecurityCrypto.generate_node_certificate(self.node_id)
                ciphertext = SecurityCrypto.encrypt_payload(message)
                payload_to_send = {"encrypted_payload": base64.b64encode(ciphertext).decode('utf-8')}
                data = json.dumps(payload_to_send).encode()
            else:
                data = json.dumps(message).encode()
                
            writer.write(data)
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            metrics.inc_sent()
            return True
        except (ConnectionRefusedError, asyncio.TimeoutError):
            # Normal in distributed systems when node is down
            return False
        except Exception as e:
            self.logger.error(f"Error sending message to {target_node_id}: {e}")
            return False

    def simulate_partition(self, target_node_ids: list):
        """Put nodes into partitioned state (cannot send/receive)"""
        for nid in target_node_ids:
            self.partitioned_nodes.add(nid)
        self.logger.info(f"Network partition simulated with: {self.partitioned_nodes}")

    def heal_partition(self, target_node_ids: list):
        """Remove nodes from partitioned state"""
        for nid in target_node_ids:
            if nid in self.partitioned_nodes:
                self.partitioned_nodes.remove(nid)
        self.logger.info(f"Network partition healed. Remaining partitions: {self.partitioned_nodes}")
