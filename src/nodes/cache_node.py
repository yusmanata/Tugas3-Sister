import asyncio
import collections
from typing import Dict, Any
from src.communication.message_passing import MessagePasser
from src.utils.metrics import setup_logger, metrics
from src.utils.config import DEFAULT_CLUSTER

class CacheLine:
    def __init__(self, key: str, value: Any, state: str = 'I'):
        self.key = key
        self.value = value
        self.state = state

class CacheNode:
    def __init__(self, node_id: str, cluster_config: dict = DEFAULT_CLUSTER, cache_size: int = 3):
        self.node_id = node_id
        host, port = cluster_config[node_id]
        
        self.logger = setup_logger(f"Cache-{node_id}")
        self.comm = MessagePasser(node_id, host, port)
        self.comm.set_peers(cluster_config)
        self.comm.register_handler(self.handle_message)
        
        # LRU Cache using OrderedDict: key -> CacheLine
        self.cache: collections.OrderedDict[str, CacheLine] = collections.OrderedDict()
        self.cache_size = cache_size
        
        # Simulated Main Memory via Redis (or Dict fallback)
        self.is_memory_controller = (node_id == list(cluster_config.keys())[0])
        self.memory_controller_id = list(cluster_config.keys())[0]
        
        import redis
        import os
        redis_host = os.environ.get("REDIS_HOST", "localhost")
        redis_port = int(os.environ.get("REDIS_PORT", 6379))
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

        # For waiting on bus responses
        self.pending_requests = {} # req_id -> Future
        self.bus_responses = {} # req_id -> list of responses

    async def start(self):
        await self.comm.start_server()

    async def stop(self):
        await self.comm.stop_server()

    # --- LRU Management ---
    async def _evict_if_needed(self):
        if len(self.cache) > self.cache_size:
            # Pop the first item (least recently used)
            key, line = self.cache.popitem(last=False)
            self.logger.info(f"Evicting key {key} (State: {line.state}) due to LRU")
            metrics.inc_cache_evictions()
            
            if line.state == 'M':
                # Writeback to main memory
                self.logger.debug(f"Writeback {key} to memory during eviction")
                await self._write_memory(key, line.value)

    def _mark_accessed(self, key: str):
        if key in self.cache:
            self.cache.move_to_end(key)

    # --- Main Memory Simulation via Redis ---
    async def _read_memory(self, key: str) -> Any:
        if self.is_memory_controller:
            try:
                val = self.redis_client.get(key)
                if val is not None:
                    try:
                        import json
                        return json.loads(val)
                    except:
                        return val
                return None
            except redis.ConnectionError:
                self.logger.warning("Redis not available, cannot read.")
                return None
        else:
            req_id = f"mem_rd_{key}"
            fut = asyncio.get_event_loop().create_future()
            self.pending_requests[req_id] = fut
            await self.comm.send_message(self.memory_controller_id, {"type": "MEM_READ", "key": key, "req_id": req_id})
            try:
                return await asyncio.wait_for(fut, timeout=2.0)
            except asyncio.TimeoutError:
                return None

    async def _write_memory(self, key: str, value: Any):
        if self.is_memory_controller:
            try:
                import json
                self.redis_client.set(key, json.dumps(value))
            except redis.ConnectionError:
                self.logger.warning("Redis not available, cannot write.")
        else:
            await self.comm.send_message(self.memory_controller_id, {"type": "MEM_WRITE", "key": key, "value": value})

    # --- Bus Communication ---
    async def _broadcast(self, msg: dict) -> list:
        req_id = msg['req_id']
        self.bus_responses[req_id] = []
        
        # Send to all peers
        for peer in self.comm.peers:
            await self.comm.send_message(peer, msg)
            
        # Wait a bit for responses to arrive
        await asyncio.sleep(0.3)
        responses = self.bus_responses.pop(req_id, [])
        return responses

    # --- Local Cache Operations (API) ---
    async def read(self, key: str) -> Any:
        line = self.cache.get(key)
        
        if line and line.state in ['M', 'E', 'S']:
            # Cache Hit
            metrics.inc_cache_hits()
            self.logger.info(f"Read Hit for {key} (State: {line.state})")
            self._mark_accessed(key)
            return line.value
            
        # Cache Miss
        metrics.inc_cache_misses()
        self.logger.info(f"Read Miss for {key}")
        
        req_id = f"bus_rd_{key}_{asyncio.get_event_loop().time()}"
        responses = await self._broadcast({"type": "BusRd", "key": key, "req_id": req_id})
        
        valid_responses = [r for r in responses if r.get('has_data')]
        
        new_state = 'S'
        value = None
        
        if valid_responses:
            # Another cache has it
            value = valid_responses[0]['value']
            # If any had M, we just become S (they also become S and writeback)
            new_state = 'S'
        else:
            # No cache has it, read from memory
            value = await self._read_memory(key)
            new_state = 'E' # Exclusive because only we have it
            
        # Insert to cache
        if key in self.cache:
            self.cache[key].value = value
            self.cache[key].state = new_state
            self._mark_accessed(key)
        else:
            self.cache[key] = CacheLine(key, value, new_state)
            await self._evict_if_needed()
            
        return value

    async def write(self, key: str, value: Any):
        line = self.cache.get(key)
        
        if line and line.state == 'M':
            metrics.inc_cache_hits()
            self.logger.info(f"Write Hit for {key} (State: M)")
            line.value = value
            self._mark_accessed(key)
            return
            
        if line and line.state == 'E':
            metrics.inc_cache_hits()
            self.logger.info(f"Write Hit for {key} (State: E -> M)")
            line.value = value
            line.state = 'M'
            self._mark_accessed(key)
            return
            
        if line and line.state == 'S':
            metrics.inc_cache_hits()
            self.logger.info(f"Write Hit for {key} (State: S -> M), broadcasting BusUpgr")
            # Upgrade: invalidate others
            req_id = f"bus_upgr_{key}_{asyncio.get_event_loop().time()}"
            await self._broadcast({"type": "BusUpgr", "key": key, "req_id": req_id})
            
            line.value = value
            line.state = 'M'
            self._mark_accessed(key)
            return
            
        # Cache Miss (I or not present)
        metrics.inc_cache_misses()
        self.logger.info(f"Write Miss for {key}, broadcasting BusRdX")
        req_id = f"bus_rdx_{key}_{asyncio.get_event_loop().time()}"
        await self._broadcast({"type": "BusRdX", "key": key, "req_id": req_id})
        
        if key in self.cache:
            self.cache[key].value = value
            self.cache[key].state = 'M'
            self._mark_accessed(key)
        else:
            self.cache[key] = CacheLine(key, value, 'M')
            await self._evict_if_needed()

    # --- Message Handling ---
    async def handle_message(self, message: dict):
        msg_type = message.get("type")
        
        if msg_type == "BusRd":
            await self._handle_bus_rd(message)
        elif msg_type == "BusRdX":
            await self._handle_bus_rdx(message)
        elif msg_type == "BusUpgr":
            await self._handle_bus_upgr(message)
        elif msg_type == "BusReply":
            req_id = message.get("req_id")
            if req_id in self.bus_responses:
                self.bus_responses[req_id].append(message)
        elif msg_type == "MEM_READ" and self.is_memory_controller:
            key = message['key']
            val = None
            try:
                raw_val = self.redis_client.get(key)
                if raw_val:
                    import json
                    val = json.loads(raw_val)
            except:
                pass
            await self.comm.send_message(message['sender_id'], {"type": "MEM_REPLY", "req_id": message['req_id'], "value": val})
        elif msg_type == "MEM_WRITE" and self.is_memory_controller:
            try:
                import json
                self.redis_client.set(message['key'], json.dumps(message['value']))
            except:
                pass
        elif msg_type == "MEM_REPLY":
            req_id = message.get("req_id")
            if req_id in self.pending_requests:
                if not self.pending_requests[req_id].done():
                    self.pending_requests[req_id].set_result(message.get("value"))

    async def _handle_bus_rd(self, message: dict):
        key = message['key']
        req_id = message['req_id']
        sender_id = message['sender_id']
        
        line = self.cache.get(key)
        if line and line.state != 'I':
            self.logger.debug(f"Snooped BusRd for {key}. My state: {line.state}. Replying and moving to S.")
            # Reply with data
            await self.comm.send_message(sender_id, {
                "type": "BusReply", 
                "req_id": req_id, 
                "has_data": True, 
                "value": line.value,
                "from_state": line.state
            })
            
            if line.state == 'M':
                # Writeback
                await self._write_memory(key, line.value)
                
            line.state = 'S'
        else:
            await self.comm.send_message(sender_id, {"type": "BusReply", "req_id": req_id, "has_data": False})

    async def _handle_bus_rdx(self, message: dict):
        key = message['key']
        req_id = message['req_id']
        sender_id = message['sender_id']
        
        line = self.cache.get(key)
        if line and line.state != 'I':
            self.logger.debug(f"Snooped BusRdX for {key}. My state: {line.state}. Replying and invalidating.")
            # Reply with data
            await self.comm.send_message(sender_id, {
                "type": "BusReply", 
                "req_id": req_id, 
                "has_data": True, 
                "value": line.value,
                "from_state": line.state
            })
            
            if line.state == 'M':
                # Writeback
                await self._write_memory(key, line.value)
                
            line.state = 'I'
            del self.cache[key] # Remove from cache to act as Invalid
        else:
            await self.comm.send_message(sender_id, {"type": "BusReply", "req_id": req_id, "has_data": False})

    async def _handle_bus_upgr(self, message: dict):
        key = message['key']
        line = self.cache.get(key)
        if line and line.state != 'I':
            self.logger.debug(f"Snooped BusUpgr for {key}. Invalidating.")
            line.state = 'I'
            del self.cache[key]
