import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pytest
from src.communication.message_passing import MessagePasser
from src.consensus.pbft import PBFTNode

@pytest.mark.asyncio
async def test_pbft():
    # Setup custom 4-node cluster for PBFT (f=1 requires N=4)
    cluster_config = {
        'node_1': ('127.0.0.1', 9001),
        'node_2': ('127.0.0.1', 9002),
        'node_3': ('127.0.0.1', 9003),
        'node_4': ('127.0.0.1', 9004),
    }

    # Execution logs to verify consensus
    execution_logs = {
        'node_1': [],
        'node_2': [],
        'node_3': [],
        'node_4': []
    }

    def make_apply_cb(node_id):
        async def cb(command):
            execution_logs[node_id].append(command)
        return cb

    print("Initializing 4-node PBFT cluster (Node 1=Primary, Node 4=Malicious)...")
    
    passers = {}
    pbft_nodes = {}
    
    for nid in cluster_config:
        host, port = cluster_config[nid]
        passer = MessagePasser(nid, host, port)
        passer.set_peers(cluster_config)
        passers[nid] = passer
        
        is_primary = (nid == 'node_1')
        is_malicious = (nid == 'node_4')
        
        peers = [p for p in cluster_config.keys() if p != nid]
        pbft = PBFTNode(nid, peers, passer, make_apply_cb(nid), is_primary, is_malicious)
        pbft_nodes[nid] = pbft

    # Start servers
    for passer in passers.values():
        await passer.start_server()
        
    for pbft in pbft_nodes.values():
        await pbft.start()
        
    await asyncio.sleep(1)

    print("\n--- Test 1: Normal Operation with Byzantine Node ---")
    command = {"action": "transfer", "amount": 100}
    
    # Send client request to primary
    await pbft_nodes['node_1'].handle_message({
        "type": "CLIENT_REQUEST",
        "command": command,
        "client_id": "client_A"
    })
    
    # Wait for PBFT consensus to finish
    print("Waiting for PBFT consensus (Pre-Prepare -> Prepare -> Commit)...")
    await asyncio.sleep(3)
    
    # Verification
    print("\nExecution Results:")
    for nid, log in execution_logs.items():
        print(f"{nid} executed: {log}")
        
    # Honest nodes should execute
    assert len(execution_logs['node_1']) == 1
    assert len(execution_logs['node_2']) == 1
    assert len(execution_logs['node_3']) == 1
    
    # Malicious node also executes internally because it receives valid commits from 1,2,3?
    # Actually, a malicious node's internal state doesn't matter for the system's correctness.
    # What matters is 1,2,3 reached consensus despite 4 lying.
    
    print("\nConsensus Reached! Honest nodes successfully executed the command despite the malicious node.")

    print("\nStopping nodes...")
    for passer in passers.values():
        await passer.stop_server()
    print("Tests completed successfully!")
