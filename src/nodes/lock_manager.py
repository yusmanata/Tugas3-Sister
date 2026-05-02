import asyncio
from typing import Dict, Set
from src.communication.message_passing import MessagePasser
from src.consensus.raft import RaftNode
from src.utils.metrics import setup_logger, metrics
from src.utils.config import DEFAULT_CLUSTER

class LockManager:
    def __init__(self, node_id: str, cluster_config: dict = DEFAULT_CLUSTER):
        self.node_id = node_id
        host, port = cluster_config[node_id]
        
        self.logger = setup_logger(f"LockMgr-{node_id}")
        self.comm = MessagePasser(node_id, host, port)
        self.comm.set_peers(cluster_config)
        
        peers = list(cluster_config.keys())
        peers.remove(node_id)
        self.raft = RaftNode(node_id, peers, self.comm, self._apply_command)

        # State Machine (Lock state)
        # resource_id -> {'mode': 'shared'/'exclusive', 'owners': set(), 'wait_queue': [{'client_id': ..., 'mode': ...}]}
        self.locks = {} 
        self.wait_for_graph = {} # client_id -> set of client_ids it is waiting for

    async def start(self):
        await self.comm.start_server()
        await self.raft.start()

    async def stop(self):
        await self.comm.stop_server()

    def _detect_deadlock(self, new_client: str, waiting_for: Set[str]) -> bool:
        """Detect cycle using DFS on Wait-For Graph"""
        graph = {k: set(v) for k, v in self.wait_for_graph.items()}
        
        # Add temporary edge for testing cycle
        if new_client not in graph:
            graph[new_client] = set()
        graph[new_client].update(waiting_for)

        visited = set()
        rec_stack = set()

        def dfs(node):
            if node in rec_stack:
                return True
            if node in visited:
                return False
            
            visited.add(node)
            rec_stack.add(node)
            
            for neighbor in graph.get(node, []):
                if dfs(neighbor):
                    return True
                    
            rec_stack.remove(node)
            return False

        # Run DFS from new_client to see if adding its edges creates a cycle
        if dfs(new_client):
            return True
        return False

    def _update_wait_graph(self):
        """Rebuild wait graph from locks state"""
        self.wait_for_graph.clear()
        for res, state in self.locks.items():
            owners = state['owners']
            for waiter in state['wait_queue']:
                w_id = waiter['client_id']
                if w_id not in self.wait_for_graph:
                    self.wait_for_graph[w_id] = set()
                self.wait_for_graph[w_id].update(owners)

    async def _apply_command(self, command: dict, client_id: str, request_id: str):
        op = command.get('op')
        res = command.get('resource')
        mode = command.get('mode')

        self.logger.info(f"Applying {op} on {res} by {client_id} (mode={mode})")

        if op == 'acquire':
            if res not in self.locks:
                # Resource is free
                self.locks[res] = {'mode': mode, 'owners': {client_id}, 'wait_queue': []}
                self.logger.info(f"Lock acquired: {res} by {client_id}")
                metrics.inc_locks_acquired()
                self._update_wait_graph()
                return

            state = self.locks[res]
            
            # Check if can share
            if state['mode'] == 'shared' and mode == 'shared' and not state['wait_queue']:
                state['owners'].add(client_id)
                self.logger.info(f"Shared lock acquired: {res} by {client_id}")
                metrics.inc_locks_acquired()
                self._update_wait_graph()
                return

            # Need to wait. Check for deadlock first.
            waiting_for = state['owners']
            if self._detect_deadlock(client_id, waiting_for):
                self.logger.warning(f"Deadlock detected! Rejecting lock request for {res} by {client_id}")
                metrics.inc_locks_rejected()
                return

            # No deadlock, add to wait queue
            self.logger.info(f"Client {client_id} waiting for lock {res}")
            state['wait_queue'].append({'client_id': client_id, 'mode': mode})
            self._update_wait_graph()

        elif op == 'release':
            if res in self.locks:
                state = self.locks[res]
                if client_id in state['owners']:
                    state['owners'].remove(client_id)
                    self.logger.info(f"Lock released: {res} by {client_id}")
                    
                    if not state['owners']:
                        # Give lock to next in queue
                        if state['wait_queue']:
                            next_req = state['wait_queue'].pop(0)
                            next_client = next_req['client_id']
                            next_mode = next_req['mode']
                            
                            state['mode'] = next_mode
                            state['owners'].add(next_client)
                            self.logger.info(f"Lock granted to waiting client: {res} to {next_client}")
                            metrics.inc_locks_acquired()
                            
                            # If it's a shared lock, we can potentially grant to other waiting readers
                            if next_mode == 'shared':
                                i = 0
                                while i < len(state['wait_queue']):
                                    if state['wait_queue'][i]['mode'] == 'shared':
                                        q_req = state['wait_queue'].pop(i)
                                        state['owners'].add(q_req['client_id'])
                                        self.logger.info(f"Shared lock also granted to: {q_req['client_id']}")
                                        metrics.inc_locks_acquired()
                                    else:
                                        break
                        else:
                            # Resource is free
                            del self.locks[res]
                    self._update_wait_graph()

    # Client API
    async def acquire_lock(self, resource: str, client_id: str, mode: str = 'exclusive'):
        if self.raft.state != 'LEADER':
            self.logger.warning("Cannot acquire lock: Not the leader")
            return False
            
        command = {'op': 'acquire', 'resource': resource, 'mode': mode}
        msg = {'type': 'CLIENT_COMMAND', 'command': command, 'client_id': client_id}
        # Simulate local dispatch to raft
        await self.raft.handle_message(msg)
        return True

    async def release_lock(self, resource: str, client_id: str):
        if self.raft.state != 'LEADER':
            self.logger.warning("Cannot release lock: Not the leader")
            return False

        command = {'op': 'release', 'resource': resource}
        msg = {'type': 'CLIENT_COMMAND', 'command': command, 'client_id': client_id}
        await self.raft.handle_message(msg)
        return True
