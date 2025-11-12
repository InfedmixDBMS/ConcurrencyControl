from typing import  Dict
import math

class TransactionManager:
    def __init__(self):
        self.transactions = {}

    def create_transaction(self, transaction_id: int) -> int:
        self.transactions[transaction_id] = {
            "start_timestamp": transaction_id,
            "validation_timestamp": math.inf,
            "finish_timestamp": math.inf,
            "read_set": set(),
            "write_set": set(),
        }
        return transaction_id

    def log_read(self, transaction_id: int, object_name: str):
        self.transactions[transaction_id]["read_set"].add(object_name)

    def log_write(self, transaction_id: int, object_name: str):
        self.transactions[transaction_id]["write_set"].add(object_name)

    def get_transaction(self, transaction_id: int) -> Dict:
        return self.transactions.get(transaction_id)

    def delete_transaction(self, transaction_id: int):
        if transaction_id in self.transactions:
            del self.transactions[transaction_id]
