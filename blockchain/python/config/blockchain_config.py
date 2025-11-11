import os
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

load_dotenv()

class BlockchainConfig:

    NETWORK = os.getenv('BLOCKCHAIN_NETWORK', 'mumbai')
    
    NETWORKS = {
        'mumbai': {
            'name': 'Polygon Mumbai Testnet',
            'rpc_url': os.getenv(
                'MUMBAI_RPC_URL',
                f"https://polygon-mumbai.g.alchemy.com/v2/{os.getenv('ALCHEMY_API_KEY', '')}"
            ),
            'chain_id': 80001,
            'explorer': 'https://mumbai.polygonscan.com',
            'currency': 'MATIC',
            'faucet': 'https://faucet.polygon.technology/'
        },
        'polygon': {
            'name': 'Polygon Mainnet',
            'rpc_url': os.getenv(
                'POLYGON_RPC_URL',
                f"https://polygon-mainnet.g.alchemy.com/v2/{os.getenv('ALCHEMY_API_KEY', '')}"
            ),
            'chain_id': 137,
            'explorer': 'https://polygonscan.com',
            'currency': 'MATIC',
            'faucet': None
        },
        'localhost': {
            'name': 'Local Hardhat Network',
            'rpc_url': 'http://127.0.0.1:8545',
            'chain_id': 1337,
            'explorer': None,
            'currency': 'ETH',
            'faucet': None
        }
    }
    
    CONTRACT_ADDRESS = os.getenv('CONTRACT_ADDRESS', '')
    CONTRACT_ABI_PATH = os.getenv(
        'CONTRACT_ABI_PATH',
        str(Path(__file__).parent.parent.parent / 'deployment' / 'DataProvenance.abi.json')
    )
    
    PRIVATE_KEY = os.getenv('PRIVATE_KEY', '')

    GAS_LIMIT = int(os.getenv('GAS_LIMIT', '500000'))
    GAS_PRICE_GWEI = float(os.getenv('GAS_PRICE_GWEI', '2.0'))
    MAX_PRIORITY_FEE_GWEI = float(os.getenv('MAX_PRIORITY_FEE_GWEI', '2.0'))

    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
    RETRY_DELAY = int(os.getenv('RETRY_DELAY', '5'))

    TRANSACTION_TIMEOUT = int(os.getenv('TRANSACTION_TIMEOUT', '300'))

    MAX_BATCH_SIZE = int(os.getenv('MAX_BATCH_SIZE', '50'))
    
    @classmethod
    def get_network_config(cls) -> Dict[str, Any]:
        return cls.NETWORKS.get(cls.NETWORK, cls.NETWORKS['mumbai'])
    
    @classmethod
    def validate_config(cls) -> bool:
        if not cls.PRIVATE_KEY:
            raise ValueError("PRIVATE_KEY not set in environment")
        
        if not cls.CONTRACT_ADDRESS:
            raise ValueError("CONTRACT_ADDRESS not set in environment")
        
        if not Path(cls.CONTRACT_ABI_PATH).exists():
            raise ValueError(f"ABI file not found: {cls.CONTRACT_ABI_PATH}")
        
        return True

config = BlockchainConfig()