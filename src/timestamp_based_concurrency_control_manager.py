import math
from .row_action import RowAction
from .concurrency_response import ConcurrencyResponse
from .concurrency_control_manager import ConcurrencyControlManager

class TimestampBasedConcurrencyControlManager(ConcurrencyControlManager):

    def __init__(self):
        super().__init__()

    def transaction_begin(self) -> int:
        transaction_id = super().begin_transaction()
        self.transactions[transaction_id] = {
            **self.transactions[transaction_id],
            'start_timestamp': math.inf,
            'validate_timestamp': math.inf,
            'finish_timestamp': math.inf,
            'read_timestamp': 0,
            'write_timestamp': 0
        }
        return transaction_id

    def transaction_commit(self, transaction_id: int) -> ConcurrencyResponse:
        # bisa additional check di sini. kalau ada yang conflict bisa panggil super().transaction_rollbak() biar
        # status transactionnya di mark sebagai "failed" (penting soalnya biar gabisa ngirim next query).
        pass

    def transaction_query(self, transaction_id: int, row_action: RowAction, row_id: int) -> ConcurrencyResponse:
        self.transaction_assert_exists(transaction_id)
        self.transaction_assert_queryable(transaction_id)
        # validasi apakah operasi dari sebuah row diperbolehkan. lihat contoh lock based concurrency control.
        pass
