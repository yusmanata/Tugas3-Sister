import hashlib
import bisect

class ConsistentHashRing:
    def __init__(self, virtual_nodes=100):
        self.virtual_nodes = virtual_nodes
        self.ring = []  # List of tuples: (hash_value, node_id)
        self.nodes = set()

    def _hash(self, key: str) -> int:
        # Gunakan MD5 untuk hashing yang cukup mendistribusikan
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

    def add_node(self, node_id: str):
        if node_id in self.nodes:
            return
        
        self.nodes.add(node_id)
        for i in range(self.virtual_nodes):
            v_node_key = f"{node_id}#{i}"
            h = self._hash(v_node_key)
            bisect.insort(self.ring, (h, node_id))

    def remove_node(self, node_id: str):
        if node_id not in self.nodes:
            return
        
        self.nodes.remove(node_id)
        self.ring = [(h, n) for h, n in self.ring if n != node_id]

    def get_node(self, key: str) -> str:
        if not self.ring:
            return None
            
        h = self._hash(key)
        # Cari node pertama yang hash-nya >= hash(key)
        idx = bisect.bisect_left(self.ring, (h, ""))
        
        # Jika melewati ujung kanan, putar kembali ke awal cincin
        if idx == len(self.ring):
            idx = 0
            
        return self.ring[idx][1]

    def get_replicas(self, key: str, count: int) -> list:
        """
        Mengembalikan daftar node untuk replikasi secara berurutan.
        count = jumlah total node yang diinginkan (termasuk primary).
        """
        if not self.ring:
            return []
            
        primary_node = self.get_node(key)
        replicas = [primary_node]
        
        if len(self.nodes) < count:
            count = len(self.nodes)
            
        # Jika count = 1, cukup primary node
        if count == 1:
            return replicas

        # Cari index dari primary
        h = self._hash(key)
        idx = bisect.bisect_left(self.ring, (h, ""))
        if idx == len(self.ring):
            idx = 0

        # Berjalan sepanjang cincin untuk mencari node berbeda
        curr_idx = idx
        while len(replicas) < count:
            curr_idx = (curr_idx + 1) % len(self.ring)
            node = self.ring[curr_idx][1]
            if node not in replicas:
                replicas.append(node)
                
        return replicas
