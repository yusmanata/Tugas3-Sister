import os

# Konfigurasi default cluster untuk Raft
# Menggunakan localhost dengan port yang berbeda untuk simulasi
def get_cluster_config():
    env_cluster = os.environ.get('CLUSTER_NODES')
    if env_cluster:
        # Expected format: node_1:10.0.0.1:8000,node_2:10.0.0.2:8000
        cluster = {}
        for node_str in env_cluster.split(','):
            if not node_str.strip(): continue
            parts = node_str.strip().split(':')
            if len(parts) == 3:
                node_id, host, port = parts
                cluster[node_id] = (host, int(port))
        return cluster
    
    # Fallback to local simulation
    return {
        'node_1': ('127.0.0.1', 8001),
        'node_2': ('127.0.0.1', 8002),
        'node_3': ('127.0.0.1', 8003),
    }

DEFAULT_CLUSTER = get_cluster_config()

# Timeout dalam detik
RAFT_ELECTION_TIMEOUT_MIN = 1.5
RAFT_ELECTION_TIMEOUT_MAX = 3.0
RAFT_HEARTBEAT_INTERVAL = 0.5

# Konfigurasi komunikasi
NETWORK_BUFFER_SIZE = 4096
