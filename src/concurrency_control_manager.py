from abc import ABC, classmethod
from .action import Action
from .response import Response

class ConcurrencyControlManager(ABC):
    def __init__(self, failure_recover_manager):
        self.failure_recover_manager = failure_recover_manager

    @classmethod
    def begin_transaction(self) -> int:
        pass

    @classmethod
    def end_transaction(self, transaction_id: int) -> None:
        pass

    @classmethod
    def log_object(self, object, transaction_id: int) -> None:
        pass

    @classmethod
    def validate_object(self, object, transaction_id: int, action: Action) -> Response:
        pass
