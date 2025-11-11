import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from python.services.blockchain_client import BlockchainProvenanceClient
from python.utils.hash_utils import HashUtils

# Mock data
MOCK_CONTRACT_ADDRESS = "0x" + "1" * 40
MOCK_PRIVATE_KEY = "0x" + "a" * 64
MOCK_RPC_URL = "http://localhost:8545"

@pytest.fixture
def mock_web3():
    with patch('python.services.blockchain_client.Web3') as mock:
        web3_instance = Mock()
        web3_instance.is_connected.return_value = True
        web3_instance.eth.chain_id = 80001
        web3_instance.eth.get_balance.return_value = 1000000000000000000
        mock.return_value = web3_instance
        yield web3_instance

@pytest.fixture
def client(mock_web3):
    with patch('python.services.blockchain_client.config') as mock_config:
        mock_config.CONTRACT_ADDRESS = MOCK_CONTRACT_ADDRESS
        mock_config.PRIVATE_KEY = MOCK_PRIVATE_KEY
        mock_config.CONTRACT_ABI_PATH = "test_abi.json"
        
        # Create mock ABI file
        with patch('builtins.open', create=True) as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = json.dumps([])
            
            client = BlockchainProvenanceClient(
                contract_address=MOCK_CONTRACT_ADDRESS,
                private_key=MOCK_PRIVATE_KEY,
                rpc_url=MOCK_RPC_URL
            )
            
            yield client

def test_hash_generation():
    data = {"key": "value"}
    hash1 = HashUtils.sha256_hash(data)
    hash2 = HashUtils.sha256_hash(data)
    
    assert hash1 == hash2
    assert len(hash1) == 64
    assert isinstance(hash1, str)

def test_hash_verification():
    data = "test data"
    hash_value = HashUtils.sha256_hash(data)
    
    assert HashUtils.verify_hash(data, hash_value)
    assert not HashUtils.verify_hash("different data", hash_value)

def test_log_ingestion(client):
    with patch.object(client.tx_manager, 'send_transaction') as mock_send:
        with patch.object(client.tx_manager, 'wait_for_receipt') as mock_receipt:
            mock_send.return_value = "0x123"
            mock_receipt.return_value = {
                'status': 1,
                'blockNumber': 12345,
                'gasUsed': 50000,
                'effectiveGasPrice': 2000000000
            }
            
            result = client.log_ingestion(
                data_id="test-001",
                data={"test": "data"},
                source="test-source"
            )
            
            assert result['success']
            assert result['data_id'] == "test-001"
            assert 'tx_hash' in result
            assert 'data_hash' in result

def test_log_transformation(client):
    with patch.object(client.tx_manager, 'send_transaction') as mock_send:
        with patch.object(client.tx_manager, 'wait_for_receipt') as mock_receipt:
            mock_send.return_value = "0x456"
            mock_receipt.return_value = {
                'status': 1,
                'blockNumber': 12346,
                'gasUsed': 60000,
                'effectiveGasPrice': 2000000000
            }
            
            result = client.log_transformation(
                data_id="test-002",
                data={"transformed": "data"},
                transformation_type="CLEAN",
                source_data_id="test-001"
            )
            
            assert result['success']
            assert result['transformation_type'] == "CLEAN"

def test_batch_logging(client):
    records = [
        {'data_id': f'batch-{i}', 'data': {'value': i}}
        for i in range(5)
    ]
    
    with patch.object(client.tx_manager, 'send_transaction') as mock_send:
        with patch.object(client.tx_manager, 'wait_for_receipt') as mock_receipt:
            mock_send.return_value = "0x789"
            mock_receipt.return_value = {
                'status': 1,
                'blockNumber': 12347,
                'gasUsed': 200000,
                'effectiveGasPrice': 2000000000
            }
            
            result = client.log_batch(records)
            
            assert result['success']
            assert result['record_count'] == 5
            assert 'cost_per_record' in result

if __name__ == "__main__":
    pytest.main([__file__, "-v"])