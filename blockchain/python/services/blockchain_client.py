import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from web3 import Web3
from web3.middleware import geth_poa_middleware
from eth_account import Account

from python.config.blockchain_config import config
from python.services.transaction_manager import TransactionManager
from python.services.gas_optimizer import GasOptimizer
from python.utils.hash_utils import HashUtils
from python.utils.validation import ValidationUtils

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BlockchainProvenanceClient:
    
    def __init__(
        self,
        contract_address: str = None,
        private_key: str = None,
        rpc_url: str = None
    ):

        self.contract_address = contract_address or config.CONTRACT_ADDRESS
        self.private_key = private_key or config.PRIVATE_KEY
        self.network_config = config.get_network_config()

        rpc_url = rpc_url or self.network_config['rpc_url']
        self.web3 = Web3(Web3.HTTPProvider(rpc_url))

        if config.NETWORK in ['mumbai', 'polygon']:
            self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)

        if not self.web3.is_connected():
            raise ConnectionError(f"Failed to connect to {rpc_url}")
        
        logger.info(f"Connected to {self.network_config['name']}")
        logger.info(f"Chain ID: {self.web3.eth.chain_id}")

        self.account = Account.from_key(self.private_key)
        self.address = self.account.address
        
        logger.info(f"Using address: {self.address}")

        self.tx_manager = TransactionManager(self.web3)
        self.gas_optimizer = GasOptimizer(self.web3)

        self._check_balance()
    
    def _load_contract(self):

        abi_path = Path(config.CONTRACT_ABI_PATH)
        if not abi_path.exists():
            raise FileNotFoundError(f"ABI file not found: {abi_path}")
        
        with open(abi_path, 'r') as f:
            contract_abi = json.load(f)

        if not ValidationUtils.validate_ethereum_address(self.contract_address):
            raise ValueError(f"Invalid contract address: {self.contract_address}")

        contract = self.web3.eth.contract(
            address=Web3.to_checksum_address(self.contract_address),
            abi=contract_abi
        )
        
        logger.info(f"Contract loaded: {self.contract_address}")
        
        return contract
    
    def _check_balance(self):
        balance_wei = self.web3.eth.get_balance(self.address)
        balance_matic = Web3.from_wei(balance_wei, 'ether')
        
        logger.info(f"Balance: {balance_matic} {self.network_config['currency']}")
        
        if balance_wei == 0:
            logger.warning("⚠️  Zero balance! Get testnet tokens from faucet:")
            logger.warning(f"   {self.network_config.get('faucet', 'N/A')}")
    
    def _build_transaction(
        self,
        contract_function,
        gas_limit: int = None
    ) -> Dict[str, Any]:

        nonce = self.tx_manager.get_nonce(self.address)

        if gas_limit is None:
            gas_limit = self.gas_optimizer.estimate_gas(
                contract_function,
                self.address
            )

        transaction = {
            'from': self.address,
            'nonce': nonce,
            'gas': gas_limit,
            'chainId': self.web3.eth.chain_id
        }

        if self.gas_optimizer.should_use_eip1559():
            fees = self.gas_optimizer.get_eip1559_fees()
            transaction.update(fees)
        else:
            transaction['gasPrice'] = self.gas_optimizer.get_gas_price()
        
        return transaction
    
    def log_ingestion(
        self,
        data_id: str,
        data: Any,
        source: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:

        if not ValidationUtils.validate_data_id(data_id):
            raise ValueError(f"Invalid data_id: {data_id}")

        data_hash = HashUtils.sha256_hash(data)

        metadata_json = json.dumps(metadata or {})
        metadata_json = ValidationUtils.sanitize_metadata(metadata_json)
        
        logger.info(f"Logging ingestion: {data_id}")
        logger.info(f"  Data hash: {data_hash}")
        logger.info(f"  Source: {source}")
        
        contract_function = self.contract.functions.logIngestion(
            data_id,
            data_hash,
            source,
            metadata_json
        )
        
        transaction = self._build_transaction(contract_function)
        transaction = contract_function.build_transaction(transaction)

        tx_hash = self.tx_manager.send_transaction(transaction, self.private_key)
        
        logger.info(f"Transaction sent: {tx_hash}")

        receipt = self.tx_manager.wait_for_receipt(tx_hash)

        if not self.tx_manager.verify_transaction(receipt):
            raise Exception("Transaction failed")

        cost = self.tx_manager.get_transaction_cost(receipt)
        
        logger.info(f"✓ Transaction confirmed!")
        logger.info(f"  Gas used: {cost['gas_used']}")
        logger.info(f"  Cost: {cost['cost_matic']:.6f} {self.network_config['currency']}")
        
        return {
            'success': True,
            'data_id': data_id,
            'data_hash': data_hash,
            'tx_hash': tx_hash,
            'block_number': receipt['blockNumber'],
            'gas_used': cost['gas_used'],
            'cost_matic': cost['cost_matic'],
            'explorer_url': self._get_explorer_url(tx_hash)
        }
    
    def log_transformation(
        self,
        data_id: str,
        data: Any,
        transformation_type: str,
        source_data_id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:

        if not ValidationUtils.validate_data_id(data_id):
            raise ValueError(f"Invalid data_id: {data_id}")
        
        if not ValidationUtils.validate_data_id(source_data_id):
            raise ValueError(f"Invalid source_data_id: {source_data_id}")

        data_hash = HashUtils.sha256_hash(data)

        metadata_json = json.dumps(metadata or {})
        metadata_json = ValidationUtils.sanitize_metadata(metadata_json)
        
        logger.info(f"Logging transformation: {data_id}")
        logger.info(f"  Type: {transformation_type}")
        logger.info(f"  Source: {source_data_id}")

        contract_function = self.contract.functions.logTransformation(
            data_id,
            data_hash,
            transformation_type,
            source_data_id,
            metadata_json
        )
        
        transaction = self._build_transaction(contract_function)
        transaction = contract_function.build_transaction(transaction)

        tx_hash = self.tx_manager.send_transaction(transaction, self.private_key)
        
        logger.info(f"Transaction sent: {tx_hash}")

        receipt = self.tx_manager.wait_for_receipt(tx_hash)

        if not self.tx_manager.verify_transaction(receipt):
            raise Exception("Transaction failed")

        cost = self.tx_manager.get_transaction_cost(receipt)
        
        logger.info(f"✓ Transaction confirmed!")
        logger.info(f"  Gas used: {cost['gas_used']}")
        logger.info(f"  Cost: {cost['cost_matic']:.6f} {self.network_config['currency']}")
        
        return {
            'success': True,
            'data_id': data_id,
            'data_hash': data_hash,
            'transformation_type': transformation_type,
            'source_data_id': source_data_id,
            'tx_hash': tx_hash,
            'block_number': receipt['blockNumber'],
            'gas_used': cost['gas_used'],
            'cost_matic': cost['cost_matic'],
            'explorer_url': self._get_explorer_url(tx_hash)
        }
    
    def log_storage(
        self,
        data_id: str,
        data: Any,
        storage_location: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:

        if not ValidationUtils.validate_data_id(data_id):
            raise ValueError(f"Invalid data_id: {data_id}")

        data_hash = HashUtils.sha256_hash(data)

        metadata_json = json.dumps(metadata or {})
        metadata_json = ValidationUtils.sanitize_metadata(metadata_json)
        
        logger.info(f"Logging storage: {data_id}")
        logger.info(f"  Location: {storage_location}")

        contract_function = self.contract.functions.logStorage(
            data_id,
            data_hash,
            storage_location,
            metadata_json
        )
        
        transaction = self._build_transaction(contract_function)
        transaction = contract_function.build_transaction(transaction)

        tx_hash = self.tx_manager.send_transaction(transaction, self.private_key)
        
        logger.info(f"Transaction sent: {tx_hash}")

        receipt = self.tx_manager.wait_for_receipt(tx_hash)

        if not self.tx_manager.verify_transaction(receipt):
            raise Exception("Transaction failed")

        cost = self.tx_manager.get_transaction_cost(receipt)
        
        logger.info(f"✓ Transaction confirmed!")
        logger.info(f"  Gas used: {cost['gas_used']}")
        logger.info(f"  Cost: {cost['cost_matic']:.6f} {self.network_config['currency']}")
        
        return {
            'success': True,
            'data_id': data_id,
            'data_hash': data_hash,
            'storage_location': storage_location,
            'tx_hash': tx_hash,
            'block_number': receipt['blockNumber'],
            'gas_used': cost['gas_used'],
            'cost_matic': cost['cost_matic'],
            'explorer_url': self._get_explorer_url(tx_hash)
        }
    
    def log_batch(
        self,
        records: List[Dict[str, Any]]
    ) -> Dict[str, Any]:

        if len(records) > config.MAX_BATCH_SIZE:
            raise ValueError(f"Batch size exceeds maximum: {config.MAX_BATCH_SIZE}")

        data_ids = []
        data_hashes = []
        transformation_types = []
        source_data_ids = []
        metadata_array = []
        
        for record in records:
            data_ids.append(record['data_id'])
            data_hashes.append(HashUtils.sha256_hash(record['data']))
            transformation_types.append(record.get('transformation_type', 'BATCH'))
            source_data_ids.append(record.get('source_data_id', ''))
            
            metadata_json = json.dumps(record.get('metadata', {}))
            metadata_array.append(ValidationUtils.sanitize_metadata(metadata_json))
        
        logger.info(f"Logging batch: {len(records)} records")

        contract_function = self.contract.functions.logBatch(
            data_ids,
            data_hashes,
            transformation_types,
            source_data_ids,
            metadata_array
        )
        
        transaction = self._build_transaction(contract_function)
        transaction = contract_function.build_transaction(transaction)

        tx_hash = self.tx_manager.send_transaction(transaction, self.private_key)
        
        logger.info(f"Transaction sent: {tx_hash}")

        receipt = self.tx_manager.wait_for_receipt(tx_hash)

        if not self.tx_manager.verify_transaction(receipt):
            raise Exception("Batch transaction failed")

        cost = self.tx_manager.get_transaction_cost(receipt)
        
        logger.info(f"✓ Batch transaction confirmed!")
        logger.info(f"  Records: {len(records)}")
        logger.info(f"  Gas used: {cost['gas_used']}")
        logger.info(f"  Cost: {cost['cost_matic']:.6f} {self.network_config['currency']}")
        logger.info(f"  Cost per record: {cost['cost_matic']/len(records):.6f}")
        
        return {
            'success': True,
            'record_count': len(records),
            'tx_hash': tx_hash,
            'block_number': receipt['blockNumber'],
            'gas_used': cost['gas_used'],
            'cost_matic': cost['cost_matic'],
            'cost_per_record': cost['cost_matic'] / len(records),
            'explorer_url': self._get_explorer_url(tx_hash)
        }
    
    def get_lineage(self, data_id: str) -> List[Dict[str, Any]]:

        logger.info(f"Fetching lineage for: {data_id}")
        
        try:
            records = self.contract.functions.getFullLineage(data_id).call()
            
            lineage = []
            for record in records:
                lineage.append({
                    'data_id': record[0],
                    'data_hash': record[1],
                    'transformation_type': record[2],
                    'source_data_id': record[3],
                    'processor': record[4],
                    'timestamp': record[5],
                    'metadata': json.loads(record[6]) if record[6] else {}
                })
            
            logger.info(f"Found {len(lineage)} records")
            return lineage
            
        except Exception as e:
            logger.error(f"Error fetching lineage: {e}")
            return []
    
    def verify_integrity(self, data_id: str, data: Any) -> bool:

        data_hash = HashUtils.sha256_hash(data)
        
        try:
            is_valid = self.contract.functions.verifyDataIntegrity(
                data_id,
                data_hash
            ).call()
            
            logger.info(f"Integrity check for {data_id}: {'✓ VALID' if is_valid else '✗ INVALID'}")
            return is_valid
            
        except Exception as e:
            logger.error(f"Error verifying integrity: {e}")
            return False
    
    def get_contract_stats(self) -> Dict[str, Any]:
        try:
            stats = self.contract.functions.getContractStats().call()
            
            return {
                'total_records': stats[0],
                'total_data_items': stats[1]
            }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {}
    
    def _get_explorer_url(self, tx_hash: str) -> Optional[str]:
        explorer = self.network_config.get('explorer')
        if explorer:
            return f"{explorer}/tx/{tx_hash}"
        return None

print("Blockchain Integration Layer - Part 3 of 3")
print("Main blockchain client created")
print("\\nGenerating remaining files...")
