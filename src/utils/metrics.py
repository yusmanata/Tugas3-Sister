import logging

class SystemMetrics:
    def __init__(self):
        self.messages_sent = 0
        self.messages_received = 0
        self.locks_acquired = 0
        self.locks_rejected = 0
        self.leader_changes = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.cache_evictions = 0

    def inc_sent(self):
        self.messages_sent += 1

    def inc_received(self):
        self.messages_received += 1

    def inc_locks_acquired(self):
        self.locks_acquired += 1

    def inc_locks_rejected(self):
        self.locks_rejected += 1
        
    def inc_leader_changes(self):
        self.leader_changes += 1

    def inc_cache_hits(self):
        self.cache_hits += 1
        
    def inc_cache_misses(self):
        self.cache_misses += 1
        
    def inc_cache_evictions(self):
        self.cache_evictions += 1

    def get_report(self):
        return {
            'messages_sent': self.messages_sent,
            'messages_received': self.messages_received,
            'locks_acquired': self.locks_acquired,
            'locks_rejected': self.locks_rejected,
            'leader_changes': self.leader_changes,
            'cache_hits': self.cache_hits,
            'cache_misses': self.cache_misses,
            'cache_evictions': self.cache_evictions
        }

# Global metrics instance
metrics = SystemMetrics()

def setup_logger(node_id):
    logger = logging.getLogger(node_id)
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(f'%(asctime)s - [{node_id}] - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger
