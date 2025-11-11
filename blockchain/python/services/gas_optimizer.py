import time
from typing import Dict, Any, Optional
from web3 import Web3
from python.config.blockchain_config import config

class GasOptimizer:
    
    def __init__(self, web3: Web3):
        self.web3 = web3
        self.network_config = config.get_network_config()
    
    def get_gas_price(self) -> int:

        try:
            gas_price = self.web3.eth.gas_price

            gas_price = int(gas_price * 1.1)

            max_gas_price = Web3.to_wei(config.GAS_PRICE_GWEI * 2, 'gwei')
            gas_price = min(gas_price, max_gas_price)
            
            return gas_price
            
        except Exception as e:
            print(f"Error getting gas price: {e}")
            # Fallback to configured gas price
            return Web3.to_wei(config.GAS_PRICE_GWEI, 'gwei')
    
    def estimate_gas(
        self, 
        contract_function,
        from_address: str,
        **kwargs
    ) -> int:

        try:
            estimated_gas = contract_function.estimate_gas({
                'from': from_address,
                **kwargs
            })
            
            # Add 20% buffer
            return int(estimated_gas * 1.2)
            
        except Exception as e:
            print(f"Error estimating gas: {e}")
            # Fallback to configured gas limit
            return config.GAS_LIMIT
    
    def get_eip1559_fees(self) -> Dict[str, int]:

        try:
            # Get latest block
            latest_block = self.web3.eth.get_block('latest')
            base_fee = latest_block.get('baseFeePerGas', 0)
            
            # Priority fee
            max_priority_fee = Web3.to_wei(config.MAX_PRIORITY_FEE_GWEI, 'gwei')
            
            # Max fee = base fee + priority fee
            max_fee = base_fee + max_priority_fee
            
            return {
                'maxFeePerGas': max_fee,
                'maxPriorityFeePerGas': max_priority_fee
            }
            
        except Exception as e:
            print(f"Error getting EIP-1559 fees: {e}")
            # Fallback
            return {
                'maxFeePerGas': Web3.to_wei(config.GAS_PRICE_GWEI * 2, 'gwei'),
                'maxPriorityFeePerGas': Web3.to_wei(config.MAX_PRIORITY_FEE_GWEI, 'gwei')
            }
    
    def should_use_eip1559(self) -> bool:

        try:
            latest_block = self.web3.eth.get_block('latest')
            return 'baseFeePerGas' in latest_block
        except:
            return False
    
    def wait_for_better_gas(
        self, 
        max_price_gwei: float,
        timeout: int = 300
    ) -> bool:

        start_time = time.time()
        max_price_wei = Web3.to_wei(max_price_gwei, 'gwei')
        
        while time.time() - start_time < timeout:
            current_price = self.web3.eth.gas_price
            
            if current_price <= max_price_wei:
                return True
            
            print(f"Gas price too high: {Web3.from_wei(current_price, 'gwei')} Gwei. Waiting...")
            time.sleep(30)
        
        return False