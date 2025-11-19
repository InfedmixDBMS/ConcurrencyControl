from .row_action import RowAction
from .concurrency_response import ConcurrencyResponse
from .concurrency_control_manager import ConcurrencyControlManager

class TimestampBasedConcurrencyControlManager(ConcurrencyControlManager):

    def __init__(self):
        super().__init__()
        self.row_read_timestamps = {}
        self.row_write_timestamps = {}

    def transaction_begin(self) -> int:
        transaction_id = super().transaction_begin()
        # pake transaction_id karena udah incremental
        self.transactions[transaction_id] = {
            **self.transactions[transaction_id],
            'timestamp': transaction_id,
            'read_set': set(),
            'write_set': set()
        }
        return transaction_id

    def transaction_commit(self, transaction_id: int) -> ConcurrencyResponse:
        self.transaction_assert_exists(transaction_id)
        self.transaction_assert_queryable(transaction_id)
        transaction = self.transactions[transaction_id]
        
        for row_id in transaction['read_set']:
            write_ts = self.row_write_timestamps.get(row_id, 0)
            if write_ts > transaction['timestamp']:
                # Reject: commit data yg udah ke-write ts yg lebih muda
                super().transaction_rollback(transaction_id)
                return ConcurrencyResponse(
                    transaction_id, 
                    False, 
                    f'Commit denied: row {row_id} was modified by newer transaction'
                )
        
        # commit success
        super().transaction_commit(transaction_id)
        return ConcurrencyResponse(transaction_id, True, 'Transaction committed successfully')

    def transaction_query(self, transaction_id: int, row_action: RowAction, row_id: int) -> ConcurrencyResponse:
        self.transaction_assert_exists(transaction_id)
        self.transaction_assert_queryable(transaction_id)
        
        transaction = self.transactions[transaction_id]
        ts = transaction['timestamp']
        
        # get current timestamps buat row
        read_ts = self.row_read_timestamps.get(row_id, 0)
        write_ts = self.row_write_timestamps.get(row_id, 0)
        
        if row_action == RowAction.READ:
            # Read rule: TS(Ti) >= WTS(X)
            if ts < write_ts:
                # Reject: read data yg udah ke-write ts yg lebih muda
                super().transaction_rollback(transaction_id)
                return ConcurrencyResponse(
                    transaction_id, 
                    False, 
                    f'Read denied: row {row_id} written by newer transaction (WTS={write_ts} > TS={ts})'
                )
            
            # update read timestamp ke max
            self.row_read_timestamps[row_id] = max(read_ts, ts)
            transaction['read_set'].add(row_id)
            return ConcurrencyResponse(transaction_id, True, f'Read allowed on row {row_id}')
        
        if row_action == RowAction.WRITE:
            # Write rule: TS(Ti) >= RTS(X) and TS(Ti) >= WTS(X)
            if ts < read_ts:
                # Reject: write data yg udah ke-read ts yg lebih muda
                super().transaction_rollback(transaction_id)
                return ConcurrencyResponse(
                    transaction_id, 
                    False, 
                    f'Write denied: row {row_id} read by newer transaction (RTS={read_ts} > TS={ts})'
                )
            
            if ts < write_ts:
                # Thomas Write Rule: ga peduli kalo ts > write_ts 
                transaction['write_set'].add(row_id)
                return ConcurrencyResponse(
                    transaction_id, 
                    True, 
                    f'Write ignored (Thomas Write Rule): row {row_id} already written by newer transaction'
                )
            
            # update write timestamp
            self.row_write_timestamps[row_id] = ts
            transaction['write_set'].add(row_id)
            return ConcurrencyResponse(transaction_id, True, f'Write allowed on row {row_id}')
        
        raise Exception(f'Unknown row action {row_action}')
