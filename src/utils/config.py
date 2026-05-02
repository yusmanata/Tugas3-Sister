import os

# Konfigurasi default cluster untuk Raft
# Menggunakan localhost dengan port yang berbeda untuk simulasi
def get_cluster_config(service_type: str = None):
    # Map service types to their specific environment variables
    service_map = {
        'lock': 'LOCK_CLUSTER_NODES',
        'queue': 'QUEUE_CLUSTER_NODES',
        'cache': 'CACHE_CLUSTER_NODES'
    }
    
    # If service_type is provided, prioritize that specific env var
    target_vars = [service_map[service_type]] if service_type in service_map else []
    # Add generic fallback
    target_vars.append('CLUSTER_NODES')
    
    cluster = {}
    for var in target_vars:
        env_cluster = os.environ.get(var)
        if env_cluster:
            for node_str in env_cluster.split(','):
                if not node_str.strip(): continue
                parts = node_str.strip().split(':')
                if len(parts) == 3:
                    node_id, host, port = parts
                    cluster[node_id] = (host, int(port))
            if cluster: return cluster # Return as soon as we find a match
    
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
