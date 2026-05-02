"""Benchmark runner - semua hasil disimpan ke data/benchmark_results.json"""
import sys, os, io, asyncio, time, statistics, json
import fakeredis, redis

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, '.')

redis.Redis = fakeredis.FakeRedis
import src.communication.message_passing as _mp
_mp.SecurityCrypto = None

from src.nodes.queue_node import QueueNode
from src.nodes.cache_node import CacheNode
from src.utils.consistent_hashing import ConsistentHashRing

RESULTS = {}

def st(lats):
    if not lats:
        return {}
    qs = statistics.quantiles(lats, n=100) if len(lats) >= 100 else lats
    return {
        "min_ms": round(min(lats), 3),
        "max_ms": round(max(lats), 3),
        "avg_ms": round(statistics.mean(lats), 3),
        "p50_ms": round(statistics.median(lats), 3),
        "p95_ms": round(statistics.quantiles(lats, n=20)[18], 3),
        "p99_ms": round(qs[98] if len(lats) >= 100 else max(lats), 3),
    }

def primary(topic):
    ring = ConsistentHashRing()
    for n in ['node_1', 'node_2', 'node_3']:
        ring.add_node(n)
    return ring.get_node(topic)


async def main():
    # 1. Single-Node Queue
    topic = 'topic_singlenode'
    p = primary(topic)
    node = QueueNode(p)
    await node.start()
    await asyncio.sleep(0.3)
    payload = 'A' * 256
    lats = []
    N = 200
    t0 = time.perf_counter()
    for _ in range(N):
        ts = time.perf_counter()
        await node.produce(topic, payload)
        lats.append((time.perf_counter() - ts) * 1000)
    elapsed = time.perf_counter() - t0
    await node.stop()
    RESULTS['q_single'] = {
        'scenario': 'Queue - Single Node (no replication)',
        'nodes': 1, 'operations': N, 'payload_bytes': len(payload),
        'throughput_ops_sec': round(N / elapsed, 2),
        'latency': st(lats)
    }
    r = RESULTS['q_single']
    print(f"[1/5] Single-Node Produce: {r['throughput_ops_sec']} ops/s | Avg {r['latency']['avg_ms']} ms")

    # 2. 3-Node Distributed Queue (produce with replication)
    nodes = {nid: QueueNode(nid) for nid in ['node_1', 'node_2', 'node_3']}
    for n in nodes.values():
        await n.start()
    await asyncio.sleep(0.5)
    topic = 'topic_distributed'
    p = primary(topic)
    node = nodes[p]
    payload = 'A' * 256
    lats = []
    N = 200
    t0 = time.perf_counter()
    for _ in range(N):
        ts = time.perf_counter()
        await node.produce(topic, payload)
        lats.append((time.perf_counter() - ts) * 1000)
    elapsed = time.perf_counter() - t0
    for n in nodes.values():
        await n.stop()
    RESULTS['q_dist'] = {
        'scenario': 'Queue - 3-Node Distributed (produce+replication)',
        'nodes': 3, 'operations': N, 'payload_bytes': len(payload),
        'throughput_ops_sec': round(N / elapsed, 2),
        'latency': st(lats)
    }
    r = RESULTS['q_dist']
    print(f"[2/5] 3-Node Dist Produce: {r['throughput_ops_sec']} ops/s | Avg {r['latency']['avg_ms']} ms")

    # 3. Cache Hit vs Miss
    nodes = {nid: CacheNode(nid, cache_size=100) for nid in ['node_1', 'node_2', 'node_3']}
    for n in nodes.values():
        await n.start()
    await asyncio.sleep(0.5)
    await nodes['node_1'].write('hw_key', 42)
    await asyncio.sleep(0.2)
    hit_lats = []
    for _ in range(100):
        ts = time.perf_counter()
        await nodes['node_1'].read('hw_key')
        hit_lats.append((time.perf_counter() - ts) * 1000)
    miss_lats = []
    for i in range(30):
        key = 'u' + str(i)
        await nodes['node_1'].write(key, i)
        await asyncio.sleep(0.08)
        ts = time.perf_counter()
        await nodes['node_2'].read(key)
        miss_lats.append((time.perf_counter() - ts) * 1000)
    for n in nodes.values():
        await n.stop()
    RESULTS['cache'] = {
        'scenario': 'Cache MESI - Hit vs Miss (3-Node)',
        'hit_latency': st(hit_lats),
        'miss_latency': st(miss_lats)
    }
    print(f"[3/5] Cache Hit Avg: {RESULTS['cache']['hit_latency']['avg_ms']} ms | Miss Avg: {RESULTS['cache']['miss_latency']['avg_ms']} ms")

    # 4. Payload Scalability
    rows = []
    for size in [64, 256, 1024, 4096]:
        topic = 'sz' + str(size)
        p = primary(topic)
        node = QueueNode(p)
        await node.start()
        await asyncio.sleep(0.2)
        payload = 'X' * size
        lats = []
        N = 100
        t0 = time.perf_counter()
        for _ in range(N):
            ts = time.perf_counter()
            await node.produce(topic, payload)
            lats.append((time.perf_counter() - ts) * 1000)
        elapsed = time.perf_counter() - t0
        await node.stop()
        rows.append({
            'payload_bytes': size,
            'throughput_ops_sec': round(N / elapsed, 2),
            'avg_latency_ms': round(statistics.mean(lats), 3),
            'p95_latency_ms': round(statistics.quantiles(lats, n=20)[18], 3),
        })
        print(f"[4/5] Payload {size:5d}B: {rows[-1]['throughput_ops_sec']} ops/s | Avg {rows[-1]['avg_latency_ms']} ms")
    RESULTS['scalability'] = rows

    # 5. Consume comparison
    consume_rows = []
    for label, nc in [('Single-Node', 1), ('3-Node', 3)]:
        topic = 'cons' + label.replace('-', '')
        p = primary(topic)
        nids = ['node_1', 'node_2', 'node_3'][:nc]
        if p not in nids:
            nids = [p]
        nodes = {nid: QueueNode(nid) for nid in nids}
        for n in nodes.values():
            await n.start()
        await asyncio.sleep(0.3)
        pnode = nodes[p]
        N = 100
        payload = 'P' * 256
        for _ in range(N):
            await pnode.produce(topic, payload)
        lats = []
        t0 = time.perf_counter()
        for _ in range(N):
            ts = time.perf_counter()
            await pnode.consume(topic, 'bench')
            lats.append((time.perf_counter() - ts) * 1000)
        elapsed = time.perf_counter() - t0
        for n in nodes.values():
            await n.stop()
        consume_rows.append({
            'label': label,
            'throughput_ops_sec': round(N / elapsed, 2),
            'avg_latency_ms': round(statistics.mean(lats), 3)
        })
        print(f"[5/5] Consume {label}: {consume_rows[-1]['throughput_ops_sec']} ops/s | Avg {consume_rows[-1]['avg_latency_ms']} ms")
    RESULTS['consume'] = consume_rows

    os.makedirs('data', exist_ok=True)
    with open('data/benchmark_results.json', 'w') as f:
        json.dump(RESULTS, f, indent=2)
    print("DONE - results saved to data/benchmark_results.json")


if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.run(main())
