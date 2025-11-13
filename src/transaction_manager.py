from typing import Dict, Set
import math
from enum import Enum

class TransactionStatus(Enum):
    ACTIVE = "active"
    COMMITTED = "committed"
    ABORTED = "aborted"

class TransactionManager:
    def __init__(self):
        self.transactions: Dict[int, Dict] = {}
        self.transaction_counter = 0
        self.timestamp_counter = 0
    
    def create_transaction(self) -> int:
        """Create a new transaction and return its ID"""
        self.transaction_counter += 1
        self.timestamp_counter += 1
        
        transaction_id = self.transaction_counter
        start_timestamp = self.timestamp_counter
        
        self.transactions[transaction_id] = {
            "start_timestamp": start_timestamp,
            "commit_timestamp": math.inf,
            "status": TransactionStatus.ACTIVE,
            "read_set": set(),
            "write_set": set(),
        }
        return transaction_id
    
    def get_current_timestamp(self) -> float:
        """Get current logical timestamp"""
        return self.timestamp_counter
    
    def increment_timestamp(self) -> float:
        """Increment and return the next timestamp"""
        self.timestamp_counter += 1
        return self.timestamp_counter

    def log_read(self, transaction_id: int, object_name: str):
        """Log a read operation"""
        if transaction_id in self.transactions:
            self.transactions[transaction_id]["read_set"].add(object_name)

    def log_write(self, transaction_id: int, object_name: str):
        """Log a write operation"""
        if transaction_id in self.transactions:
            self.transactions[transaction_id]["write_set"].add(object_name)

    def commit_transaction(self, transaction_id: int) -> float:
        """Commit a transaction and return commit timestamp"""
        if transaction_id not in self.transactions:
            raise ValueError(f"Transaction {transaction_id} not found")
        
        commit_timestamp = self.increment_timestamp()
        self.transactions[transaction_id]["commit_timestamp"] = commit_timestamp
        self.transactions[transaction_id]["status"] = TransactionStatus.COMMITTED
        return commit_timestamp
    
    def abort_transaction(self, transaction_id: int):
        """Abort a transaction"""
        if transaction_id not in self.transactions:
            raise ValueError(f"Transaction {transaction_id} not found")
        
        self.transactions[transaction_id]["status"] = TransactionStatus.ABORTED

    def get_transaction(self, transaction_id: int) -> Dict:
        """Get transaction metadata"""
        return self.transactions.get(transaction_id)
    
    def get_min_active_timestamp(self) -> float:
        """Get the minimum start timestamp of all active transactions"""
        active_timestamps = [
            txn["start_timestamp"] 
            for txn in self.transactions.values() 
            if txn["status"] == TransactionStatus.ACTIVE
        ]
        return min(active_timestamps) if active_timestamps else self.timestamp_counter

    def delete_transaction(self, transaction_id: int):
        """Delete transaction metadata (cleanup)"""
        if transaction_id in self.transactions:
            del self.transactions[transaction_id]
    
    def is_active(self, transaction_id: int) -> bool:
        """Check if transaction is active"""
        txn = self.transactions.get(transaction_id)
        return txn and txn["status"] == TransactionStatus.ACTIVE
