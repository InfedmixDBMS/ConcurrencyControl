
from .concurrency_control_manager import ConcurrencyControlManager
from .action import Action
from .response import Response
import math

class TimestampBasedConcurrencyControlManager(ConcurrencyControlManager):

    def __init__(self, failure_recover_manager):
        super().__init__(failure_recover_manager)
        self.transactions = {}

    def begin_transaction(self) -> int:
        transaction_id = 1 + len(self.transactions)
        self.transactions[transaction_id] = {
            'start_timestamp': math.inf,
            'validate_timestamp': math.inf,
            'finish_timestamp': math.inf,
            'read_timestamp': 0,
            'write_timestamp': 0
        }
        return transaction_id

    def end_transaction(self, transaction_id: int) -> None:
        pass

    def log_object(self, object, transaction_id: int) -> None:
        pass

    def validate_object(self, object, transaction_id: int, action: Action) -> Response:
        pass

class LockBasedConcurrencyControl(ConcurrencyControlManager):

    def __init__(self, failure_recover_manager):
        super().__init__(failure_recover_manager)
        self.transactions = {}

    def begin_transaction(self) -> int:
        transaction_id = 1 + len(self.transactions)
        self.transactions[transaction_id] = {
            'read_set': set(),
            'write_set': set()
        }
        return transaction_id

    def end_transaction(self, transaction_id: int) -> None:
        pass

    def log_object(self, object, transaction_id: int) -> None:
        pass

    def validate_object(self, object, transaction_id: int, action: Action) -> Response:
        self.transactions[transaction_id]['read_set'].add(object)
        pass
