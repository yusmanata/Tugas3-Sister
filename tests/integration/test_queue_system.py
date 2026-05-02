import asyncio
import sys
import os
import shutil

# Menambahkan root direktori ke path agar import dari src berhasil
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pytest
from src.nodes.queue_node import QueueNode, VISIBILITY_TIMEOUT

def cleanup_logs():
    data_dir = os.path.join("data", "queue_logs")
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)

@pytest.mark.asyncio
async def test_queue_system():
    cleanup_logs()
    
    print("Starting 3 queue nodes...")
    nodes = {
        'node_1': QueueNode('node_1'),
        'node_2': QueueNode('node_2'),
        'node_3': QueueNode('node_3')
    }

    for node in nodes.values():
        await node.start()

    await asyncio.sleep(1) # Wait for servers to start
    
    # Helper to find primary node for a topic
    def get_primary(topic):
        # We can just ask any node's hash ring
        return nodes['node_1'].hash_ring.get_replicas(topic, 1)[0]
        
    def get_replicas(topic, count=2):
        return nodes['node_1'].hash_ring.get_replicas(topic, count)

    print("\n--- Test 1: Produce and Consume ---")
    topic_1 = "orders"
    primary_1 = get_primary(topic_1)
    print(f"Primary for {topic_1} is {primary_1}")
    
    # Produce
    msg_id = await nodes[primary_1].produce(topic_1, '{"order_id": 123}')
    await asyncio.sleep(0.5) # Wait for replication
    
    # Verify replication
    replicas = get_replicas(topic_1)
    for rep in replicas:
        assert topic_1 in nodes[rep].queues
        assert any(m['msg_id'] == msg_id for m in nodes[rep].queues[topic_1])
    print("Produce and replication successful.")

    # Consume
    msg = await nodes[primary_1].consume(topic_1, "consumer_A")
    await asyncio.sleep(0.5) # Wait for replication of consume
    
    assert msg is not None
    assert msg['payload'] == '{"order_id": 123}'
    print("Consume successful.")
    
    # Verify it is in pending_acks on replicas
    for rep in replicas:
        assert topic_1 in nodes[rep].pending_acks
        assert msg_id in nodes[rep].pending_acks[topic_1]
        
    # ACK
    await nodes[primary_1].ack(topic_1, msg_id)
    await asyncio.sleep(0.5)
    
    # Verify it is removed from pending_acks
    for rep in replicas:
        assert msg_id not in nodes[rep].pending_acks.get(topic_1, {})
    print("ACK successful.")

    print("\n--- Test 2: At-Least-Once Delivery (Visibility Timeout) ---")
    topic_2 = "payments"
    primary_2 = get_primary(topic_2)
    
    msg_id_2 = await nodes[primary_2].produce(topic_2, '{"amount": 500}')
    await asyncio.sleep(0.2)
    
    # Consume but do not ACK
    msg2 = await nodes[primary_2].consume(topic_2, "consumer_B")
    assert msg2 is not None
    await asyncio.sleep(0.2)
    
    assert len(nodes[primary_2].queues[topic_2]) == 0
    assert msg_id_2 in nodes[primary_2].pending_acks[topic_2]
    
    print(f"Waiting for visibility timeout ({VISIBILITY_TIMEOUT} seconds)...")
    await asyncio.sleep(VISIBILITY_TIMEOUT + 1)
    
    # Should be back in queue
    assert len(nodes[primary_2].queues[topic_2]) == 1
    assert msg_id_2 not in nodes[primary_2].pending_acks[topic_2]
    print("Message re-queued successfully due to timeout.")

    print("\n--- Test 3: Node Failure and Recovery (Persistence) ---")
    topic_3 = "emails"
    primary_3 = get_primary(topic_3)
    
    await nodes[primary_3].produce(topic_3, '{"to": "user@example.com"}')
    await asyncio.sleep(0.5)
    
    print(f"Stopping node {primary_3} to simulate crash...")
    await nodes[primary_3].stop()
    
    # Restart the node
    print(f"Restarting node {primary_3}...")
    new_node = QueueNode(primary_3)
    nodes[primary_3] = new_node
    # Note: the node's init reads the AOF log
    
    assert topic_3 in new_node.queues
    assert len(new_node.queues[topic_3]) == 1
    assert new_node.queues[topic_3][0]['payload'] == '{"to": "user@example.com"}'
    print("Data recovered successfully from disk.")
    
    await new_node.start()

    print("\nStopping nodes...")
    for node in nodes.values():
        await node.stop()
    print("Tests completed successfully!")
