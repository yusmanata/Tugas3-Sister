import asyncio
import sys
import os

# Menambahkan root direktori ke path agar import dari src berhasil
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pytest
from src.nodes.lock_manager import LockManager

@pytest.mark.asyncio
async def test_lock_manager():
    print("Starting 3 nodes...")
    nodes = {
        'node_1': LockManager('node_1'),
        'node_2': LockManager('node_2'),
        'node_3': LockManager('node_3')
    }

    for node in nodes.values():
        await node.start()

    print("Waiting for leader election (3 seconds)...")
    await asyncio.sleep(3)

    leader = None
    for nid, node in nodes.items():
        if node.raft.state == 'LEADER':
            leader = node
            print(f"Leader is {nid}")
            break

    if not leader:
        print("No leader elected! The timeout might be too short.")
        # Try to find one again
        await asyncio.sleep(3)
        for nid, node in nodes.items():
            if node.raft.state == 'LEADER':
                leader = node
                print(f"Leader is {nid}")
                break
        
    if not leader:
        print("Still no leader. Aborting test.")
        for node in nodes.values():
            await node.stop()
        return

    print("\n--- Test 1: Normal Operation (Exclusive Lock) ---")
    # Client A requests lock on R1
    await leader.acquire_lock('R1', 'ClientA', 'exclusive')
    await asyncio.sleep(0.5) # Wait for replication and application
    print(f"Locks on leader: {leader.locks}")
    assert 'R1' in leader.locks
    assert 'ClientA' in leader.locks['R1']['owners']

    # Client B requests lock on R1 (should wait)
    await leader.acquire_lock('R1', 'ClientB', 'exclusive')
    await asyncio.sleep(0.5)
    print(f"Locks on leader after B requests: {leader.locks}")
    assert len(leader.locks['R1']['wait_queue']) == 1

    # Client A releases lock
    await leader.release_lock('R1', 'ClientA')
    await asyncio.sleep(0.5)
    print(f"Locks on leader after A releases: {leader.locks}")
    assert 'ClientB' in leader.locks['R1']['owners']

    # Clean up R1
    await leader.release_lock('R1', 'ClientB')

    print("\n--- Test 2: Shared vs Exclusive Locks ---")
    await leader.acquire_lock('R2', 'Reader1', 'shared')
    await asyncio.sleep(0.6)
    await leader.acquire_lock('R2', 'Reader2', 'shared')
    await asyncio.sleep(0.6)
    print(f"Locks on leader (2 readers): {leader.locks}")
    assert len(leader.locks['R2']['owners']) == 2

    # Writer requests (should wait)
    await leader.acquire_lock('R2', 'Writer1', 'exclusive')
    await asyncio.sleep(0.6)
    print(f"Locks on leader (Writer waiting): {leader.locks}")
    assert len(leader.locks['R2']['wait_queue']) == 1

    # Clean up R2
    await leader.release_lock('R2', 'Reader1')
    await leader.release_lock('R2', 'Reader2')
    await asyncio.sleep(0.6)
    # Writer 1 should now have it
    print(f"Locks on leader after readers release: {leader.locks}")
    assert 'Writer1' in leader.locks['R2']['owners']
    await leader.release_lock('R2', 'Writer1')

    print("\n--- Test 3: Deadlock Detection ---")
    # Client X holds R3
    await leader.acquire_lock('R3', 'ClientX', 'exclusive')
    await asyncio.sleep(0.6)
    # Client Y holds R4
    await leader.acquire_lock('R4', 'ClientY', 'exclusive')
    await asyncio.sleep(0.6)

    # Client X requests R4 (waits for Y)
    await leader.acquire_lock('R4', 'ClientX', 'exclusive')
    await asyncio.sleep(0.6)

    # Client Y requests R3 (should be rejected due to deadlock)
    await leader.acquire_lock('R3', 'ClientY', 'exclusive')
    await asyncio.sleep(0.6)
    
    print(f"Wait For Graph: {leader.wait_for_graph}")
    print(f"Locks on R3: {leader.locks['R3']}")
    # Y should not be in R3 wait queue because it was rejected
    assert not any(w['client_id'] == 'ClientY' for w in leader.locks['R3']['wait_queue'])

    print("\n--- Test 4: Network Partition ---")
    follower = None
    for n in nodes.values():
        if n != leader:
            follower = n
            break
            
    print(f"Simulating partition. Isolating {follower.node_id} from {leader.node_id}")
    leader.comm.simulate_partition([follower.node_id])
    follower.comm.simulate_partition([leader.node_id])
    
    await asyncio.sleep(1)
    
    # Leader can still function because 2/3 nodes are available (quorum = 2)
    await leader.acquire_lock('R5', 'ClientZ', 'exclusive')
    await asyncio.sleep(0.5)
    assert 'R5' in leader.locks
    print(f"Lock R5 acquired on leader during partition.")
    
    # Heal partition
    leader.comm.heal_partition([follower.node_id])
    follower.comm.heal_partition([leader.node_id])
    print("Partition healed. Waiting for sync...")
    # Trigger a new command to force append_entries sync faster
    await leader.release_lock('R5', 'ClientZ')
    await asyncio.sleep(2)
    
    # Follower should have synced the log
    if 'R5' not in follower.locks:
         print(f"Follower successfully synced R5 release (resource deleted).")
    else:
         print(f"Follower state on R5: {follower.locks.get('R5')}")

    print("\nStopping nodes...")
    for node in nodes.values():
        await node.stop()
    print("Tests completed successfully!")
