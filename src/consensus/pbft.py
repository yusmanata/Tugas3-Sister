import asyncio
import hashlib
import json
from typing import Callable, Dict, List
from src.utils.metrics import setup_logger

class PBFTNode:
    def __init__(self, node_id: str, peers: list, message_passer, apply_command_cb: Callable, is_primary: bool = False, is_malicious: bool = False):
        self.node_id = node_id
        self.peers = peers # List of ALL nodes including self for PBFT math, or just peers. Usually we need total N.
        # Let's assume peers is a list of OTHER nodes.
        self.total_nodes = len(self.peers) + 1
        self.f = (self.total_nodes - 1) // 3
        
        self.comm = message_passer
        self.apply_command_cb = apply_command_cb
        self.is_primary = is_primary
        self.is_malicious = is_malicious
        self.logger = setup_logger(f"PBFT-{node_id}")

        self.seq_num = 0
        
        # log[seq] = {
        #    "request": dict, 
        #    "digest": str,
        #    "pre_prepare_received": bool,
        #    "prepares": {sender: digest},
        #    "commits": {sender: digest},
        #    "executed": bool
        # }
        self.log: Dict[int, dict] = {}
        
        self.comm.register_handler(self.handle_message)

    def _hash(self, message: dict) -> str:
        # Simple hash of the command payload
        msg_str = json.dumps(message, sort_keys=True)
        return hashlib.md5(msg_str.encode()).hexdigest()

    async def start(self):
        self.logger.info(f"PBFT Node started. Total nodes: {self.total_nodes}, f (fault tolerance): {self.f}, Primary: {self.is_primary}, Malicious: {self.is_malicious}")

    async def handle_message(self, message: dict):
        msg_type = message.get('type')

        if msg_type == 'CLIENT_REQUEST':
            await self._handle_client_request(message)
        elif msg_type == 'PRE-PREPARE':
            await self._handle_pre_prepare(message)
        elif msg_type == 'PREPARE':
            await self._handle_prepare(message)
        elif msg_type == 'COMMIT':
            await self._handle_commit(message)

    async def _handle_client_request(self, message: dict):
        if not self.is_primary:
            self.logger.debug("Received CLIENT_REQUEST but not primary, ignoring (in real PBFT, forward to primary).")
            return
            
        command = message.get('command')
        client_id = message.get('client_id')
        
        self.seq_num += 1
        seq = self.seq_num
        digest = self._hash(command)
        
        self.log[seq] = {
            "command": command,
            "digest": digest,
            "client_id": client_id,
            "pre_prepare_received": True,
            "prepares": {},
            "commits": {},
            "executed": False
        }
        
        self.logger.info(f"Primary initiating PRE-PREPARE for seq {seq} with digest {digest[:8]}")
        
        pre_prepare_msg = {
            "type": "PRE-PREPARE",
            "seq": seq,
            "digest": digest,
            "command": command,
            "primary_id": self.node_id
        }
        
        for peer in self.peers:
            await self.comm.send_message(peer, pre_prepare_msg)

    async def _handle_pre_prepare(self, message: dict):
        seq = message['seq']
        digest = message['digest']
        command = message['command']
        primary_id = message['primary_id']
        
        computed_digest = self._hash(command)
        if computed_digest != digest:
            self.logger.warning(f"Invalid digest in PRE-PREPARE for seq {seq}. Rejecting.")
            return
            
        if seq not in self.log:
            self.log[seq] = {
                "command": command,
                "digest": digest,
                "client_id": message.get('client_id'), # might not be passed
                "pre_prepare_received": True,
                "prepares": {},
                "commits": {},
                "executed": False
            }
            
        self.log[seq]["pre_prepare_received"] = True
        self.logger.info(f"Accepted PRE-PREPARE for seq {seq}. Multicasting PREPARE.")
        
        prepare_digest = digest
        if self.is_malicious:
            # Send a fake digest to simulate Byzantine fault
            prepare_digest = "FAKE_DIGEST_FOR_BYZANTINE_FAULT"
            self.logger.warning(f"Malicious node sending FAKE digest in PREPARE for seq {seq}")
            
        prepare_msg = {
            "type": "PREPARE",
            "seq": seq,
            "digest": prepare_digest,
            "sender_id": self.node_id
        }
        
        # Self-record
        self.log[seq]["prepares"][self.node_id] = prepare_digest
        
        for peer in self.peers:
            await self.comm.send_message(peer, prepare_msg)
            
        await self._check_prepare_condition(seq)

    async def _handle_prepare(self, message: dict):
        seq = message['seq']
        digest = message['digest']
        sender_id = message['sender_id']
        
        if seq not in self.log:
            self.log[seq] = {
                "command": None,
                "digest": None,
                "pre_prepare_received": False,
                "prepares": {},
                "commits": {},
                "executed": False
            }
            
        self.log[seq]["prepares"][sender_id] = digest
        self.logger.debug(f"Received PREPARE for seq {seq} from {sender_id}")
        await self._check_prepare_condition(seq)

    async def _check_prepare_condition(self, seq: int):
        entry = self.log[seq]
        if not entry["pre_prepare_received"]:
            return
            
        my_digest = entry["digest"]
        
        # Count prepares matching my digest
        matching_prepares = 0
        for s_id, d in entry["prepares"].items():
            if d == my_digest:
                matching_prepares += 1
                
        # Condition: Pre-prepare matches + 2f Prepares from different backups
        # Total matching needed = 2f (which includes self if we sent one)
        # Actually standard PBFT requires 2f matching prepares from OTHER nodes, or 2f total including self.
        if matching_prepares >= 2 * self.f and not entry.get("prepared_done"):
            entry["prepared_done"] = True
            self.logger.info(f"Prepared phase complete for seq {seq}. Multicasting COMMIT.")
            
            commit_digest = my_digest
            if self.is_malicious:
                commit_digest = "FAKE_DIGEST_FOR_COMMIT"
                self.logger.warning(f"Malicious node sending FAKE digest in COMMIT for seq {seq}")
                
            commit_msg = {
                "type": "COMMIT",
                "seq": seq,
                "digest": commit_digest,
                "sender_id": self.node_id
            }
            
            # Self-record
            entry["commits"][self.node_id] = commit_digest
            
            for peer in self.peers:
                await self.comm.send_message(peer, commit_msg)
                
            await self._check_commit_condition(seq)

    async def _handle_commit(self, message: dict):
        seq = message['seq']
        digest = message['digest']
        sender_id = message['sender_id']
        
        if seq not in self.log:
            self.log[seq] = {
                "command": None,
                "digest": None,
                "pre_prepare_received": False,
                "prepares": {},
                "commits": {},
                "executed": False
            }
            
        self.log[seq]["commits"][sender_id] = digest
        self.logger.debug(f"Received COMMIT for seq {seq} from {sender_id}")
        await self._check_commit_condition(seq)

    async def _check_commit_condition(self, seq: int):
        entry = self.log.get(seq)
        if not entry or not entry.get("prepared_done") or entry.get("executed"):
            return
            
        my_digest = entry["digest"]
        
        matching_commits = 0
        for s_id, d in entry["commits"].items():
            if d == my_digest:
                matching_commits += 1
                
        # Condition: 2f + 1 commits matching the digest (including self)
        if matching_commits >= 2 * self.f + 1:
            entry["executed"] = True
            command = entry["command"]
            self.logger.info(f"Consensus reached for seq {seq}. EXECUTING command: {command}")
            if self.apply_command_cb:
                await self.apply_command_cb(command)
