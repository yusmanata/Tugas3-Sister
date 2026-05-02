import asyncio
import sys
import os

# Menambahkan root direktori ke path agar import dari src berhasil
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pytest
import fakeredis
import redis
from src.nodes.cache_node import CacheNode
from src.utils.metrics import metrics

@pytest.fixture(autouse=True)
def patch_redis(monkeypatch):
    monkeypatch.setattr("redis.Redis", fakeredis.FakeRedis)

@pytest.mark.asyncio
async def test_cache_system():
    print("Starting 3 cache nodes (Cache Size = 3)...")
    nodes = {
        'node_1': CacheNode('node_1', cache_size=3),
        'node_2': CacheNode('node_2', cache_size=3),
        'node_3': CacheNode('node_3', cache_size=3)
    }

    for node in nodes.values():
        await node.start()

    await asyncio.sleep(1) # Wait for servers to start
    
    node1 = nodes['node_1']
    node2 = nodes['node_2']
    node3 = nodes['node_3']

    print("\n--- Test 1: Basic Write and Read (Exclusive / Modified) ---")
    # Write to Node 1 (Miss -> BusRdX -> E -> M)
    await node1.write('A', 100)
    assert 'A' in node1.cache
    assert node1.cache['A'].state == 'M'
    print("Write Miss to A -> M state successful.")
    
    # Read from Node 1 (Hit -> M)
    val = await node1.read('A')
    assert val == 100
    assert node1.cache['A'].state == 'M'
    print("Read Hit to A -> M state successful.")

    print("\n--- Test 2: Invalidation and Update Propagation (Shared) ---")
    # Read from Node 2 (Miss -> BusRd -> Snooped by Node 1 -> Both become S)
    val2 = await node2.read('A')
    await asyncio.sleep(0.5) # Wait for writeback to memory to complete just in case
    
    assert val2 == 100
    assert 'A' in node2.cache
    assert node2.cache['A'].state == 'S'
    assert node1.cache['A'].state == 'S'
    print("Node 2 Read Miss -> Both Node 1 and Node 2 transitioned to S.")
    
    # Write to Node 3 (Miss -> BusRdX -> Invalidate Node 1 & 2)
    await node3.write('A', 300)
    await asyncio.sleep(0.5)
    assert node3.cache['A'].state == 'M'
    assert 'A' not in node1.cache # Invalidated and removed
    assert 'A' not in node2.cache # Invalidated and removed
    print("Node 3 Write Miss (BusRdX) -> Node 3 is M, others Invalidated.")
    
    # Node 1 Writes (Miss -> BusRdX -> Node 3 invalidated)
    await node1.write('A', 400)
    await asyncio.sleep(0.5)
    assert node1.cache['A'].state == 'M'
    assert 'A' not in node3.cache
    print("Node 1 Write Miss -> Node 1 is M, Node 3 Invalidated.")

    print("\n--- Test 3: LRU Replacement Policy ---")
    # Node 1 currently has A (1 item). Limit is 3.
    await node1.write('B', 20)
    await node1.write('C', 30)
    # Cache is now full: A, B, C. All in M state.
    assert len(node1.cache) == 3
    
    # Access A to make it MRU (Most Recently Used)
    await node1.read('A') 
    # Order should be B, C, A
    
    # Write D. Should evict B.
    await node1.write('D', 40)
    
    assert len(node1.cache) == 3
    assert 'B' not in node1.cache
    assert 'A' in node1.cache
    assert 'C' in node1.cache
    assert 'D' in node1.cache
    print("LRU Eviction successful. 'B' was evicted, 'A' survived due to recent access.")
    
    # Wait for writeback of B to memory to complete
    await asyncio.sleep(0.5)
    
    # If Node 2 reads B, it should get it from memory
    val_b = await node2.read('B')
    assert val_b == 20
    assert node2.cache['B'].state == 'E'
    print("Evicted data 'B' successfully retrieved from Main Memory by Node 2.")

    print("\n--- Metrics Report ---")
    report = metrics.get_report()
    print(f"Cache Hits: {report['cache_hits']}")
    print(f"Cache Misses: {report['cache_misses']}")
    print(f"Cache Evictions: {report['cache_evictions']}")

    print("\nStopping nodes...")
    for node in nodes.values():
        await node.stop()
    print("Tests completed successfully!")
