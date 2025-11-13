from .transaction_status import TransactionStatus
from .row_action import RowAction
from .concurrency_response import ConcurrencyResponse

class ConcurrencyControlManager:

    def __init__(self):
        self.transactions = {}

    def transaction_exists(self, transaction_id: int) -> bool:
        return transaction_id in self.transactions

    def transaction_assert_exists(self, transaction_id: int) -> None:
        if self.transaction_exists(transaction_id)
            return
        raise Exception(f'Transaction with id {transaction_id} not found')

    def transaction_get_status(self, transaction_id: int) -> TransactionStatus:
        self.transaction_assert_exists(transaction_id)
        return self.transaction_status[transaction_id]['status']

    def transaction_begin(self) -> int:
        transaction_id = len(self.transactions) + 1
        self.transactions[transaction_id] = {
            'status': TransactionStatus.ACTIVE
        }
        return transaction_id

    def transaction_end(self, transaction_id: int) -> None:
        self.transaction_assert_exists(transaction_id)
        transaction = self.transactions[transaction_id]
        if transaction['status'] not in [TransactionStatus.COMMITTED, TransactionStatus.ABORTED]:
            raise Exception(f'Transaction with id {transaction_id} cannot end without COMMIT/ROLLBACK')
        transaction['status'] = TransactionStatus.TERMINATED

    def transaction_commit(self, transaction_id: int) -> ConcurrencyResponse:
        self.transaction_assert_exists(transaction_id)
        transaction = self.transactions[transaction_id]
        if transaction['status'] != TransactionStatus.ACTIVE:
            raise Exception(f'Transaction with id {transaction_id} is not active')
        transaction['status'] = TransactionStatus.PARTIALLY_COMMITTED
        pass

    def transaction_commit_flushed(self, transaction_id: int) -> None:
        self.transaction_assert_exists(transaction_id)
        transaction = self.transactions[transaction_id]
        if transaction['status'] != TransactionStatus.PARTIALLY_COMMITTED:
            raise Exception(f'Transaction with id {transaction_id} is not partially committed')
        transaction['status'] = TransactionStatus.COMMITTED

    def transaction_rollback(self, transaction_id: int) -> None:
        self.transaction_assert_exists(transaction_id)
        transaction = self.transactions[transaction_id]
        if transaction['status'] != TransactionStatus.ACTIVE:
            raise Exception(f'Transaction with id {transaction_id} is not active')
        transaction['status'] = TransactionStatus.FAILED

    def transaction_abort(self, transaction_id: int) -> None:
        self.transaction_assert_exists(transaction_id)
        transaction = self.transactions[transaction_id]
        if transaction['status'] != TransactionStatus.FAILED:
            raise Exception(f'Transaction with id {transaction_id} is not in failed state')
        transaction['status'] = TransactionStatus.ABORTED

    def transaction_query(self, transaction_id: int, row_action: RowAction, row_id: int) -> ConcurrencyResponse:
        self.transaction_assert_exists(transaction_id)
        pass
