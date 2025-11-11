import hashlib
import json
from typing import Any, Dict

class HashUtils:
    
    @staticmethod
    def sha256_hash(data: Any) -> str:
        if isinstance(data, dict) or isinstance(data, list):
            data = json.dumps(data, sort_keys=True)
        
        if isinstance(data, str):
            data = data.encode('utf-8')
        
        return hashlib.sha256(data).hexdigest()
    
    @staticmethod
    def verify_hash(data: Any, expected_hash: str) -> bool:
        actual_hash = HashUtils.sha256_hash(data)
        return actual_hash.lower() == expected_hash.lower()
    
    @staticmethod
    def hash_file(filepath: str) -> str:
        sha256 = hashlib.sha256()
        
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                sha256.update(chunk)
        
        return sha256.hexdigest()