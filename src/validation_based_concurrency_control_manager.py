import time

from src.transaction_status import TransactionStatus
from .row_action import TableAction
from .concurrency_response import ConcurrencyResponse, LockStatus
from .concurrency_control_manager import ConcurrencyControlManager

class ValidationBasedConcurrencyControlManager(ConcurrencyControlManager):

    def __init__(self):
        super().__init__()

    def transaction_begin(self) -> int:
        transaction_id = super().transaction_begin()
        self.transactions[transaction_id] = {
            **self.transactions[transaction_id],
            'read_set': set(),
            'write_set': set(),
            'start_timestamp': time.time(),
            'validation_timestamp': None,
            'finish_timestamp': None,
        }
        return transaction_id      

    def transaction_query(self, transaction_id: int, table_action: TableAction, table_name: str) -> ConcurrencyResponse:
        self.transaction_assert_exists(transaction_id)
        self.transaction_assert_queryable(transaction_id)
        if table_action == TableAction.READ:
            self.transactions[transaction_id]['read_set'].add(table_name)
            return ConcurrencyResponse(transaction_id, True, 'Read successful', LockStatus.GRANTED)
        if table_action == TableAction.WRITE:
            self.transactions[transaction_id]['write_set'].add(table_name,)
            return ConcurrencyResponse(transaction_id, True, 'Write successful', LockStatus.GRANTED)
        raise Exception(f'Unknown table action {table_action}')
    
    def transaction_commit_flushed(self, transaction_id):
        super().transaction_commit_flushed(transaction_id)
        Ti = self.transactions[transaction_id]
        Ti['finish_timestamp'] = time.time()

    def transaction_commit(self, transaction_id: int) -> ConcurrencyResponse:
        self.transaction_assert_exists(transaction_id)
        self.transaction_assert_queryable(transaction_id)
        Ti = self.transactions[transaction_id]

        Ti['validation_timestamp'] = time.time()

        for other_id, Tj in self.transactions.items():
            if other_id == transaction_id:
                continue
            
            # Only validate against committed transactions
            if Tj['status'] not in [TransactionStatus.COMMITTED, TransactionStatus.TERMINATED]:
                continue
            
            if Tj['finish_timestamp'] <= Ti['start_timestamp']:
                continue
            if Tj['start_timestamp'] >= Ti['validation_timestamp']:
                continue
            
            # read-write or write-write
            if (Tj['write_set'] & Ti['read_set']) or (Tj['write_set'] & Ti['write_set']):
                Ti['status'] = TransactionStatus.ABORTED
                return ConcurrencyResponse(
                    transaction_id, False,
                    f"Validation failed due to conflict with transaction {other_id}",
                    LockStatus.FAILED
                )
        
        # Passed all validation checks
        Ti['status'] = TransactionStatus.PARTIALLY_COMMITTED
        return ConcurrencyResponse(transaction_id, True, "Validation successful", LockStatus.GRANTED)
