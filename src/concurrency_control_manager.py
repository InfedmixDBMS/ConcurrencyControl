from abc import ABC, abstractmethod
from .action import Action
from .response import Response

class ConcurrencyControlManager(ABC):
    def __init__(self, failure_recover_manager):
        self.failure_recover_manager = failure_recover_manager

    @abstractmethod
    def begin_transaction(self) -> int:
        pass

    @abstractmethod
    def end_transaction(self, transaction_id: int) -> None:
        pass

    @abstractmethod
    def log_object(self, object, transaction_id: int) -> None:
        pass

    @abstractmethod
    def validate_object(self, object, transaction_id: int, action: Action) -> Response:
        pass
