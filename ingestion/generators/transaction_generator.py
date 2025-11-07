import uuid
from datetime import datetime
from typing import Dict, Any, List
from faker import Faker
import random

class TransactionGenerator:
    #Generates transaction data for ingestion
    def __init__(self, seed: int = 42):
        self.fake = Faker()
        Faker.seed(seed)
        random.seed(seed)

        self.transaction_ids = [str(uuid.uuid4()) for _ in range(1000)]
        self.merchants = ['Amazon', 'Walmart', 'Target', 'eBay', 'Best Buy', 
                          'Apple Store', 'Google Play', 'Netflix', 'Spotify']
        self.payment_methods = ['credit_card', 'debit_card', 'bank_transfer', 'digital_wallet', 'cash']
    
    def generate_transaction(self) -> Dict[str, Any]:
        #Generates a single transaction record
        transaction_id = str(uuid.uuid4())
        amount = round(random.uniform(5.0, 500.0), 2)
        merchants = random.choice(self.merchants)
        payment_methods = random.choice(self.payment_methods)
        timestamp = datetime.utcnow()

        transaction = {
            "transaction_id": transaction_id,
            "user_id": str(uuid.uuid4()),
            "merchants": random.choice(self.merchants),
            "amount": amount,
            "timestamp": timestamp.isoformat(),
            "payment_method": random.choice(self.payment_methods),
            "status": random.choice(['completed', 'pending', 'failed']),
            "location": {
                "city": self.fake.city(),
                "country": self.fake.country()
            }
        }
        return transaction
    
    def generate_batch(self, n: int = 10) -> List[Dict[str, Any]]:
        #Generates a batch of transaction records
        return [self.generate_transaction() for _ in range(n)]