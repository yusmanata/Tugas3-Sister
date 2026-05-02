import asyncio
import random
from typing import Callable, List
from src.utils.config import RAFT_ELECTION_TIMEOUT_MIN, RAFT_ELECTION_TIMEOUT_MAX, RAFT_HEARTBEAT_INTERVAL
from src.utils.metrics import setup_logger, metrics

class RaftNode:
    def __init__(self, node_id: str, peers: list, message_passer, apply_command_cb: Callable):
        self.node_id = node_id
        self.peers = peers
        self.comm = message_passer
        self.apply_command_cb = apply_command_cb
        self.logger = setup_logger(f"Raft-{node_id}")

        # Persistent state
        self.current_term = 0
        self.voted_for = None
        self.log = []  # List of dicts: {'term': int, 'command': dict}

        # Volatile state
        self.commit_index = 0
        self.last_applied = 0
        self.state = 'FOLLOWER'

        # Volatile state on leaders
        self.next_index = {peer: 1 for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}

        self.election_timer = None
        self.heartbeat_timer = None
        self.votes_received = 0

        # Register message handler
        self.comm.register_handler(self.handle_message)

    async def start(self):
        self.reset_election_timer()
        asyncio.create_task(self._apply_loop())

    def reset_election_timer(self):
        if self.election_timer:
            self.election_timer.cancel()
        timeout = random.uniform(RAFT_ELECTION_TIMEOUT_MIN, RAFT_ELECTION_TIMEOUT_MAX)
        self.election_timer = asyncio.get_event_loop().call_later(timeout, self._trigger_election)

    def _trigger_election(self):
        if self.state == 'LEADER':
            return
        asyncio.create_task(self.start_election())

    async def start_election(self):
        self.state = 'CANDIDATE'
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = 1
        self.logger.info(f"Starting election for term {self.current_term}")
        self.reset_election_timer()

        last_log_index = len(self.log)
        last_log_term = self.log[-1]['term'] if self.log else 0

        request_vote_msg = {
            'type': 'REQUEST_VOTE',
            'term': self.current_term,
            'candidate_id': self.node_id,
            'last_log_index': last_log_index,
            'last_log_term': last_log_term
        }

        for peer in self.peers:
            await self.comm.send_message(peer, request_vote_msg)

    async def _send_heartbeats(self):
        while self.state == 'LEADER':
            for peer in self.peers:
                prev_log_index = self.next_index[peer] - 1
                prev_log_term = self.log[prev_log_index - 1]['term'] if prev_log_index > 0 else 0
                entries = self.log[prev_log_index:]

                append_entries_msg = {
                    'type': 'APPEND_ENTRIES',
                    'term': self.current_term,
                    'leader_id': self.node_id,
                    'prev_log_index': prev_log_index,
                    'prev_log_term': prev_log_term,
                    'entries': entries,
                    'leader_commit': self.commit_index
                }
                await self.comm.send_message(peer, append_entries_msg)
            await asyncio.sleep(RAFT_HEARTBEAT_INTERVAL)

    def become_leader(self):
        self.state = 'LEADER'
        metrics.inc_leader_changes()
        self.logger.info(f"Became LEADER for term {self.current_term}")
        if self.election_timer:
            self.election_timer.cancel()
        
        self.next_index = {peer: len(self.log) + 1 for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}
        
        # Start sending heartbeats
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        self.heartbeat_timer = asyncio.create_task(self._send_heartbeats())

    async def handle_message(self, message: dict):
        msg_type = message.get('type')
        term = message.get('term', 0)

        if term > self.current_term:
            self.current_term = term
            self.state = 'FOLLOWER'
            self.voted_for = None
            if self.heartbeat_timer:
                self.heartbeat_timer.cancel()
            self.reset_election_timer()

        if msg_type == 'REQUEST_VOTE':
            await self._handle_request_vote(message)
        elif msg_type == 'VOTE_RESPONSE':
            await self._handle_vote_response(message)
        elif msg_type == 'APPEND_ENTRIES':
            await self._handle_append_entries(message)
        elif msg_type == 'APPEND_ENTRIES_RESPONSE':
            await self._handle_append_entries_response(message)
        elif msg_type == 'CLIENT_COMMAND':
            await self._handle_client_command(message)

    async def _handle_request_vote(self, message: dict):
        candidate_id = message['candidate_id']
        term = message['term']
        last_log_index = message['last_log_index']
        last_log_term = message['last_log_term']

        my_last_log_index = len(self.log)
        my_last_log_term = self.log[-1]['term'] if self.log else 0

        vote_granted = False
        if term >= self.current_term:
            # Check if candidate's log is at least as up-to-date as ours
            log_ok = (last_log_term > my_last_log_term) or \
                     (last_log_term == my_last_log_term and last_log_index >= my_last_log_index)
            
            if (self.voted_for is None or self.voted_for == candidate_id) and log_ok:
                vote_granted = True
                self.voted_for = candidate_id
                self.reset_election_timer()

        response = {
            'type': 'VOTE_RESPONSE',
            'term': self.current_term,
            'vote_granted': vote_granted
        }
        await self.comm.send_message(candidate_id, response)

    async def _handle_vote_response(self, message: dict):
        if self.state != 'CANDIDATE':
            return
        
        if message.get('vote_granted'):
            self.votes_received += 1
            if self.votes_received > (len(self.peers) + 1) / 2:
                self.become_leader()

    async def _handle_append_entries(self, message: dict):
        leader_id = message['leader_id']
        term = message['term']
        prev_log_index = message['prev_log_index']
        prev_log_term = message['prev_log_term']
        entries = message['entries']
        leader_commit = message['leader_commit']

        if term < self.current_term:
            await self.comm.send_message(leader_id, {'type': 'APPEND_ENTRIES_RESPONSE', 'term': self.current_term, 'success': False})
            return

        self.reset_election_timer()

        # Log consistency check
        if prev_log_index > 0:
            if len(self.log) < prev_log_index or self.log[prev_log_index - 1]['term'] != prev_log_term:
                await self.comm.send_message(leader_id, {'type': 'APPEND_ENTRIES_RESPONSE', 'term': self.current_term, 'success': False})
                return

        # Truncate and append
        if entries:
            self.log = self.log[:prev_log_index]
            self.log.extend(entries)

        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log))

        await self.comm.send_message(leader_id, {
            'type': 'APPEND_ENTRIES_RESPONSE', 
            'term': self.current_term, 
            'success': True,
            'match_index': len(self.log)
        })

    async def _handle_append_entries_response(self, message: dict):
        if self.state != 'LEADER':
            return

        sender_id = message['sender_id']
        if message['success']:
            self.match_index[sender_id] = message.get('match_index', 0)
            self.next_index[sender_id] = self.match_index[sender_id] + 1

            # Update commit index
            match_indices = sorted(list(self.match_index.values()) + [len(self.log)], reverse=True)
            # Find the index that a majority has reached
            majority_index = match_indices[len(self.peers) // 2]
            
            if majority_index > self.commit_index and self.log[majority_index - 1]['term'] == self.current_term:
                self.commit_index = majority_index
                self.logger.info(f"Commit index advanced to {self.commit_index}")
        else:
            self.next_index[sender_id] = max(1, self.next_index[sender_id] - 1)

    async def _handle_client_command(self, message: dict):
        if self.state != 'LEADER':
            self.logger.debug(f"Not leader, ignoring client command")
            return
        
        command = message['command']
        client_id = message.get('client_id')
        request_id = message.get('request_id')

        # Append to log
        entry = {'term': self.current_term, 'command': command, 'client_id': client_id, 'request_id': request_id}
        self.log.append(entry)
        self.logger.debug(f"Appended client command to log at index {len(self.log)}")
        
        # Will be replicated by heartbeat timer

    async def _apply_loop(self):
        while True:
            if self.commit_index > self.last_applied:
                self.last_applied += 1
                entry = self.log[self.last_applied - 1]
                command = entry['command']
                self.logger.info(f"Applying command {self.last_applied}: {command}")
                await self.apply_command_cb(command, entry.get('client_id'), entry.get('request_id'))
            await asyncio.sleep(0.01)

