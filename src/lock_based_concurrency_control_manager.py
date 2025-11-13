from .row_action import RowAction
from .concurrency_response import ConcurrencyResponse
from .concurrency_control_manager import ConcurrencyControlManager

class LockBasedConcurrencyControlManager(ConcurrencyControlManager):

    def __init__(self):
        super().__init__()
        self.shared_locks = {}
        self.exclusive_locks = {}

    def transaction_begin(self) -> int:
        transaction_id = super().transaction_begin()
        self.transactions[transaction_id] = {
            **self.transactions[transaction_id],
            'shared_row_ids': set(),
            'exclusive_row_ids': set()
        }
        return transaction_id

    def __transaction_release_locks(self, transaction_id: int) -> None:
        transaction = self.transactions[transaction_id]
        for row_id in transaction['shared_row_ids']:
            shared_holders = self.shared_locks[row_id] if row_id in self.shared_locks else None
            if shared_holders is None:
                continue
            shared_holders.discard(transaction_id)
            if len(shared_holders) == 0:
                del self.shared_locks[row_id]
        for row_id in transaction['exclusive_row_ids']:
            exclusive_holder = self.exclusive_locks[row_id] if row_id in self.exclusive_locks else None
            if exclusive_holder != transaction_id:
                continue
            del self.exclusive_locks[row_id]

    def transaction_commit_flushed(self, transaction_id: int) -> None:
        super().transaction_commit_flushed(transaction_id)
        self.__transaction_release_locks(transaction_id)

    def transaction_abort(self, transaction_id: int) -> None:
        super().transaction_abort(transaction_id)
        self.__transaction_release_locks(transaction_id)

    def transaction_query(self, transaction_id: int, row_action: RowAction, row_id: int) -> ConcurrencyResponse:
        self.transaction_assert_exists(transaction_id)
        self.transaction_assert_queryable(transaction_id)
        transaction = self.transactions[transaction_id]
        shared_holders = self.shared_locks[row_id] if row_id in self.shared_locks else None
        exclusive_holder = self.exclusive_locks[row_id] if row_id in self.exclusive_locks else None
        if row_action == RowAction.READ:
            if exclusive_holder is not None:
                if exclusive_holder != transaction_id:
                    return ConcurrencyResponse(transaction_id, False, f'Read denied: exclusive lock held by transaction {exclusive_holder}')
                # exclusive_holder is already transaction_id, no need to add to shared_holders
            else:
                if shared_holders is None:
                    shared_holders = set()
                    self.shared_locks[row_id] = shared_holders
                transaction['shared_row_ids'].add(row_id)
                shared_holders.add(transaction_id)
            return ConcurrencyResponse(transaction_id, True, 'Read lock granted')
        if row_action == RowAction.WRITE:
            if exclusive_holder == transaction_id:
                return ConcurrencyResponse(transaction_id, True, 'Write lock already held (exclusive)')
            if exclusive_holder is not None:
                return ConcurrencyResponse(transaction_id, False, f'Write denied: exclusive lock held by transaction id {exclusive_holder}')
            # exclusive_holder is none
            if shared_holders is not None:
                other_shared_holders = shared_holders - {transaction_id}
                if len(other_shared_holders) > 0:
                    return ConcurrencyResponse(transaction_id, False, f'Write denied: read locks held by other transactions {next(iter(other_shared_holders))}')
                # remove from shared_holders and we will put it in exclusive_holder. essentially upgrading the lock
                shared_holders.discard(transaction_id)
                transaction['shared_row_ids'].discard(row_id)
            transaction['exclusive_row_ids'].add(row_id)
            self.exclusive_locks[row_id] = transaction_id
            return ConcurrencyResponse(transaction_id, True, 'Write lock granted (upgraded or new)')
        raise Exception(f'Unknown row action {row_action}')
