import os
from cryptography.fernet import Fernet
import json
import base64

# Shared Secret Key for symmetric encryption across the cluster
# We prioritize the ENCRYPTION_KEY from the environment variables (set in .env)
_env_key = os.environ.get('ENCRYPTION_KEY')
if _env_key:
    SHARED_SECRET = _env_key.encode()
else:
    SHARED_SECRET = b'JgU3b1vR3WbY4xV5T8Z9bA2_cFdEgH_IjKlMnOpQrStU=' 

try:
    _fernet = Fernet(SHARED_SECRET)
except Exception:
    # If key is invalid, use a fixed valid fallback key so all nodes still match
    # A 32-byte base64 encoded key
    _fallback = b'u-Y_D5H0fF_Q3Y9_z1A2B3C4D5E6F7G8H9I0J1K2L3M='
    _fernet = Fernet(_fallback)

class SecurityCrypto:
    @staticmethod
    def encrypt_payload(payload: dict) -> bytes:
        """Encrypt JSON payload for end-to-end security."""
        json_str = json.dumps(payload)
        ciphertext = _fernet.encrypt(json_str.encode('utf-8'))
        return ciphertext

    @staticmethod
    def decrypt_payload(ciphertext: bytes) -> dict:
        """Decrypt payload received from the network."""
        try:
            json_str = _fernet.decrypt(ciphertext).decode('utf-8')
            return json.loads(json_str)
        except Exception as e:
            raise ValueError("Decryption failed or invalid payload") from e

    @staticmethod
    def generate_node_certificate(node_id: str) -> str:
        """
        Simulate a node certificate. 
        In a real system, this would be an X.509 cert.
        Here we create a signed token to prove identity.
        """
        token_data = {"id": node_id, "type": "node_cert"}
        return _fernet.encrypt(json.dumps(token_data).encode()).decode()

    @staticmethod
    def verify_node_certificate(token: str, expected_id: str = None) -> bool:
        """Verify the node certificate token."""
        try:
            data = json.loads(_fernet.decrypt(token.encode()).decode())
            if data.get("type") != "node_cert":
                return False
            if expected_id and data.get("id") != expected_id:
                return False
            return True
        except Exception:
            return False
