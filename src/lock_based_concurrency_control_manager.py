from .row_action import RowAction
from .concurrency_response import ConcurrencyResponse
from .concurrency_control_manager import ConcurrencyControlManager

class LockBasedConcurrencyControl(ConcurrencyControlManager):

    def __init__(self):
        super().__init__()

    def transaction_begin(self) -> int:
        transaction_id = super().begin_transaction()
        self.transactions[transaction_id] = {
            **self.transactions[transaction_id],
            'read_set': set(),
            'write_set': set()
        }
        return transaction_id

    def transaction_commit(self, transaction_id: int) -> ConcurrencyResponse:
        pass

    def transaction_query(self, transaction_id: int, row_action: RowAction, row_id: int) -> ConcurrencyResponse:
        self.transaction_assert_exists(transaction_id)
        pass
