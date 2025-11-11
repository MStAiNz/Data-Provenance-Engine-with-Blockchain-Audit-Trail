import time
from typing import Dict, Any, Optional
from web3 import Web3
from web3.exceptions import TransactionNotFound
from python.config.blockchain_config import config

class TransactionManager:
    
    def __init__(self, web3: Web3):
        self.web3 = web3
    
    def send_transaction(
        self,
        transaction: Dict[str, Any],
        private_key: str
    ) -> str:

        signed_txn = self.web3.eth.account.sign_transaction(
            transaction,
            private_key
        )
        
        tx_hash = self.web3.eth.send_raw_transaction(signed_txn.rawTransaction)
        
        return tx_hash.hex()
    
    def wait_for_receipt(
        self,
        tx_hash: str,
        timeout: int = None,
        poll_latency: float = 1.0
    ) -> Dict[str, Any]:
        
        timeout = timeout or config.TRANSACTION_TIMEOUT
        
        receipt = self.web3.eth.wait_for_transaction_receipt(
            tx_hash,
            timeout=timeout,
            poll_latency=poll_latency
        )
        
        return dict(receipt)
    
    def verify_transaction(self, receipt: Dict[str, Any]) -> bool:
       
        return receipt.get('status') == 1
    
    def get_transaction_cost(self, receipt: Dict[str, Any]) -> Dict[str, Any]:
        
        gas_used = receipt.get('gasUsed', 0)
        effective_gas_price = receipt.get('effectiveGasPrice', 0)
        
        cost_wei = gas_used * effective_gas_price
        cost_matic = Web3.from_wei(cost_wei, 'ether')
        
        return {
            'gas_used': gas_used,
            'gas_price_gwei': Web3.from_wei(effective_gas_price, 'gwei'),
            'cost_wei': cost_wei,
            'cost_matic': float(cost_matic),
            'cost_usd': 0.0  # Would need price oracle
        }
    
    def retry_transaction(
        self,
        transaction_func,
        max_retries: int = None,
        retry_delay: int = None
    ) -> Optional[str]:

        max_retries = max_retries or config.MAX_RETRIES
        retry_delay = retry_delay or config.RETRY_DELAY
        
        for attempt in range(max_retries):
            try:
                tx_hash = transaction_func()
                return tx_hash
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                
                wait_time = retry_delay * (2 ** attempt)
                print(f"Transaction failed (attempt {attempt + 1}/{max_retries}): {e}")
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
        
        return None
    
    def get_nonce(self, address: str) -> int:

        return self.web3.eth.get_transaction_count(address, 'pending')
    
    def cancel_transaction(
        self,
        nonce: int,
        from_address: str,
        private_key: str
    ) -> str:

        gas_price = int(self.web3.eth.gas_price * 1.2)
        
        # Create cancellation transaction
        cancel_tx = {
            'nonce': nonce,
            'to': from_address,  # Send to self
            'value': 0,
            'gas': 21000,
            'gasPrice': gas_price
        }
        
        return self.send_transaction(cancel_tx, private_key)
    
print("Blockchain Integration Layer - Part 2 of 3")
print("Configuration and utility modules created")
print("\\nNext: Main blockchain client implementation...")