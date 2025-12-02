from .row_action import TableAction
from .concurrency_response import ConcurrencyResponse, LockStatus
from .concurrency_control_manager import ConcurrencyControlManager

class TimestampBasedConcurrencyControlManager(ConcurrencyControlManager):

    def __init__(self):
        super().__init__()
        self.table_read_timestamps = {}
        self.table_write_timestamps = {}

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
        
        for table_name in transaction['read_set']:
            write_ts = self.table_write_timestamps.get(table_name, 0)
            if write_ts > transaction['timestamp']:
                # Reject: commit data yg udah ke-write ts yg lebih muda
                super().transaction_rollback(transaction_id)
                return ConcurrencyResponse(
                    transaction_id, 
                    f'Commit denied: table {table_name} was modified by newer transaction',
                    LockStatus.FAILED
                )
        
        # commit success
        super().transaction_commit(transaction_id)
        return ConcurrencyResponse(transaction_id, 'Transaction committed successfully', LockStatus.GRANTED)

    def transaction_query(self, transaction_id: int, table_action: TableAction, table_name: str) -> ConcurrencyResponse:
        self.transaction_assert_exists(transaction_id)
        self.transaction_assert_queryable(transaction_id)
        
        transaction = self.transactions[transaction_id]
        ts = transaction['timestamp']
        
        # get current timestamps buat table
        read_ts = self.table_read_timestamps.get(table_name, 0)
        write_ts = self.table_write_timestamps.get(table_name, 0)
        
        if table_action == TableAction.READ:
            # Read rule: TS(Ti) >= WTS(X)
            if ts < write_ts:
                # Reject: read data yg udah ke-write ts yg lebih muda
                super().transaction_rollback(transaction_id)
                return ConcurrencyResponse(
                    transaction_id, 
                    f'Read denied: table {table_name} written by newer transaction (WTS={write_ts} > TS={ts})',
                    LockStatus.FAILED
                )
            
            # update read timestamp ke max
            self.table_read_timestamps[table_name] = max(read_ts, ts)
            transaction['read_set'].add(table_name)
            return ConcurrencyResponse(transaction_id, f'Read allowed on table {table_name}', LockStatus.GRANTED)
        
        if table_action == TableAction.WRITE:
            # Write rule: TS(Ti) >= RTS(X) and TS(Ti) >= WTS(X)
            if ts < read_ts:
                # Reject: write data yg udah ke-read ts yg lebih muda
                super().transaction_rollback(transaction_id)
                return ConcurrencyResponse(
                    transaction_id, 
                    f'Write denied: table {table_name} read by newer transaction (RTS={read_ts} > TS={ts})',
                    LockStatus.FAILED
                )
            
            if ts < write_ts:
                # Thomas Write Rule: ga peduli kalo ts > write_ts 
                transaction['write_set'].add(table_name)
                return ConcurrencyResponse(
                    transaction_id, 
                    f'Write ignored (Thomas Write Rule): table {table_name} already written by newer transaction',
                    LockStatus.GRANTED
                )
            
            # update write timestamp
            self.table_write_timestamps[table_name] = ts
            transaction['write_set'].add(table_name)
            return ConcurrencyResponse(transaction_id, f'Write allowed on table {table_name}', LockStatus.GRANTED)
        
        raise Exception(f'Unknown table action {table_action}')
