import json
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, List
from faker import Faker
import random


class UserGenerator:
    #Generates synthetic user data for ingestion
    def __init__(self, seed: int = 42):
        self.fake = Faker()
        Faker.seed(seed)
        random.seed(seed)

        self.user_ids = [str(uuid.uuid4()) for _ in range(1000)]
        self.actions = ['login', 'logout', 'view_page', 'click_ad', 'click_button', 
                       'submit_form', 'download_file', 'upload_file', 
                       'search', 'update_profile', 'delete_item']
        self.pages = ['/home', '/about', '/contact', '/products', '/services', 
                      '/terms', '/privacy', '/dashboard']
    
    def generate_user_event(self) -> Dict[str, Any]:
        #Generates a single user event
        event_id = str(uuid.uuid4())
        user_id = random.choice(self.user_ids)
        action = random.choice(self.actions)
        timestamp = datetime.utcnow()

        event = {
            "event_id": event_id,
            "user_id": user_id,
            "action": action,
            "timestamp": timestamp.isoformat(),
            "ip_address": self.fake.ipv4(),
            "user_agent": self.fake.user_agent(),
            "session_id": str(uuid.uuid4()),
            "page_url": random.choice(self.pages),
            "referrer": self.fake.url() if random.random() < 0.5 else None,
            "duration_seconds": random.randint(1, 300) if action == 'view_page' else None,
            "city": self.fake.city(),
            "country": self.fake.country()
        }
        return event
    
    def generate_batch(self, n: int = 10) -> List[Dict[str, Any]]:
        #Generates a batch of user events
        return [self.generate_user_event() for _ in range(n)]


