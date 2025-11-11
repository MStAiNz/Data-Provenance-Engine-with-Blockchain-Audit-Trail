import re
from typing import Optional

class ValidationUtils:
    
    @staticmethod
    def validate_data_id(data_id: str) -> bool:
       
        if not data_id or len(data_id) == 0:
            return False
        
        if len(data_id) > 256:
            return False
        
        # Allow alphanumeric, hyphens, underscores
        pattern = r'^[a-zA-Z0-9_-]+'

        return bool(re.match(pattern, data_id))
    
    @staticmethod
    def validate_hash(hash_string: str) -> bool:
        if not hash_string or len(hash_string) != 64:
            return False

        pattern = r'^[a-fA-F0-9]{64}'

        return bool(re.match(pattern, hash_string))
    
    @staticmethod
    def validate_ethereum_address(address: str) -> bool:
        if not address:
            return False

        pattern = r'^0x[a-fA-F0-9]{40}'

        return bool(re.match(pattern, address))
    
    @staticmethod
    def sanitize_metadata(metadata: str, max_length: int = 1024) -> str:
        if not metadata:
            return '{}'

        if len(metadata) > max_length:
            metadata = metadata[:max_length]

        metadata = metadata.replace('\\x00', '')
        
        return metadata