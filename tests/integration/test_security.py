import asyncio
import sys
import os
import json

# Menambahkan root direktori ke path agar import dari src berhasil
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pytest
from src.security.rbac import RBACManager
from src.security.audit import TamperProofLogger
from src.communication.message_passing import MessagePasser
from src.security.crypto import SecurityCrypto

@pytest.mark.asyncio
async def test_security():
    print("--- Test 1: Role-Based Access Control (RBAC) ---")
    
    # Test Admin access
    print("Testing client_A (Admin)...")
    assert RBACManager.check_permission("client_A", "write") == True
    assert RBACManager.check_permission("client_A", "delete") == True
    print("Client_A has Admin privileges.")

    # Test Guest access
    print("Testing client_C (Guest)...")
    assert RBACManager.check_permission("client_C", "read") == True
    assert RBACManager.check_permission("client_C", "write") == False
    
    try:
        RBACManager.require_permission("client_C", "write")
        assert False, "Should have raised PermissionError"
    except PermissionError as e:
        print(f"RBAC Enforcement working: {e}")


    print("\n--- Test 2: Tamper-Proof Audit Logging ---")
    # Clean up old logs for test
    log_file = "data/audit_logs/test_node_audit.log"
    if os.path.exists(log_file):
        os.remove(log_file)
        
    logger = TamperProofLogger("test_node")
    
    # Log some events
    logger.log_event("USER_LOGIN", "client_A", {"ip": "192.168.1.1"})
    logger.log_event("DATA_READ", "client_C", {"resource": "queue_1"})
    logger.log_event("DATA_WRITE", "client_A", {"resource": "queue_1", "bytes": 500})
    
    # Verify chain
    is_valid = logger.verify_chain()
    assert is_valid == True
    print("Audit Log Chain verified successfully (3 valid entries).")
    
    # Tamper with the log manually
    print("Tampering with log file (changing client_C to client_A on line 2)...")
    with open(log_file, 'r') as f:
        lines = f.readlines()
        
    # Modify second line (index 1)
    lines[1] = lines[1].replace("client_C", "client_A")
    
    with open(log_file, 'w') as f:
        f.writelines(lines)
        
    # Verify chain again
    is_valid = logger.verify_chain()
    assert is_valid == False
    print("Tamper Detection successful! The system identified the broken hash chain.")


    print("\n--- Test 3: End-to-End Encryption & Node Authentication ---")
    
    # We will simulate a raw read from the network to prove it's encrypted
    server_started = asyncio.Event()
    received_raw_data = []

    async def dummy_server(reader, writer):
        data = await reader.read(4096)
        received_raw_data.append(data)
        writer.close()
        
    server = await asyncio.start_server(dummy_server, '127.0.0.1', 9999)
    print("Started dummy sniffer server on port 9999")
    
    # Send using our MessagePasser (which uses crypto internally)
    passer = MessagePasser("node_x", "127.0.0.1", 9998)
    passer.set_peers({"node_target": ('127.0.0.1', 9999)})
    
    secret_message = {"action": "SECRET_TRANSFER", "amount": 1000000}
    await passer.send_message("node_target", secret_message)
    
    await asyncio.sleep(0.5)
    server.close()
    
    raw_bytes = received_raw_data[0]
    raw_payload = json.loads(raw_bytes.decode())
    
    print("\nIntercepted Network Payload:")
    print(json.dumps(raw_payload, indent=2))
    
    assert "encrypted_payload" in raw_payload
    assert "SECRET_TRANSFER" not in raw_bytes.decode()
    print("\nEncryption Verified: The secret payload was not visible in plain text.")
    
    # Decrypt to prove it works
    ciphertext = __import__('base64').b64decode(raw_payload["encrypted_payload"])
    decrypted = SecurityCrypto.decrypt_payload(ciphertext)
    print("\nDecrypted Data at destination:")
    print(decrypted)
    
    assert decrypted["action"] == "SECRET_TRANSFER"
    assert decrypted["auth_token"] is not None
    
    # Prove cert verification works
    is_cert_valid = SecurityCrypto.verify_node_certificate(decrypted["auth_token"], expected_id="node_x")
    assert is_cert_valid == True
    print("Node Authentication Certificate verified successfully.")

    print("\nSecurity Tests completed successfully!")
