import hashlib
import json
import os
import time

class TamperProofLogger:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.log_dir = os.path.join("data", "audit_logs")
        os.makedirs(self.log_dir, exist_ok=True)
        self.log_file = os.path.join(self.log_dir, f"{node_id}_audit.log")
        
        # Load the last hash
        self.last_hash = self._get_last_hash()

    def _get_last_hash(self) -> str:
        """Read the log file from the bottom to find the last hash."""
        if not os.path.exists(self.log_file):
            return "GENESIS_HASH_0000000000000000000000000"
            
        try:
            with open(self.log_file, 'r') as f:
                lines = f.readlines()
                if not lines:
                    return "GENESIS_HASH_0000000000000000000000000"
                    
                last_line = lines[-1].strip()
                # Assuming format: {"event": ..., "prev_hash": ..., "hash": ...}
                log_entry = json.loads(last_line)
                return log_entry.get("hash", "ERROR")
        except Exception:
            return "ERROR"

    def _calculate_hash(self, event_data: dict, prev_hash: str) -> str:
        data_str = json.dumps(event_data, sort_keys=True)
        combined = f"{data_str}|{prev_hash}"
        return hashlib.sha256(combined.encode('utf-8')).hexdigest()

    def log_event(self, event_type: str, user_id: str, details: dict):
        """Log a security or system event in a tamper-proof way."""
        event_data = {
            "timestamp": time.time(),
            "event_type": event_type,
            "user_id": user_id,
            "details": details
        }
        
        current_hash = self._calculate_hash(event_data, self.last_hash)
        
        log_entry = {
            "event": event_data,
            "prev_hash": self.last_hash,
            "hash": current_hash
        }
        
        # Write to file
        with open(self.log_file, 'a') as f:
            f.write(json.dumps(log_entry) + '\n')
            
        # Update last hash
        self.last_hash = current_hash

    def verify_chain(self) -> bool:
        """Verify the integrity of the entire log chain."""
        if not os.path.exists(self.log_file):
            return True # Empty is valid
            
        current_prev_hash = "GENESIS_HASH_0000000000000000000000000"
        
        with open(self.log_file, 'r') as f:
            for line_idx, line in enumerate(f):
                line = line.strip()
                if not line: continue
                
                try:
                    entry = json.loads(line)
                    event_data = entry["event"]
                    recorded_prev_hash = entry["prev_hash"]
                    recorded_hash = entry["hash"]
                    
                    if recorded_prev_hash != current_prev_hash:
                        print(f"Audit Chain Broken at line {line_idx+1}: prev_hash mismatch")
                        return False
                        
                    calculated_hash = self._calculate_hash(event_data, current_prev_hash)
                    
                    if calculated_hash != recorded_hash:
                        print(f"Audit Chain Broken at line {line_idx+1}: calculated hash mismatch")
                        return False
                        
                    current_prev_hash = recorded_hash
                    
                except Exception as e:
                    print(f"Audit Chain Broken at line {line_idx+1}: parsing error - {e}")
                    return False
                    
        return True
