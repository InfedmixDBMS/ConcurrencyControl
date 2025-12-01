from enum import Enum

class LockStatus(Enum):
    GRANTED = 'granted'           #lock acquired, proceed
    WAITING = 'waiting'           #transaction should wait and retry
    FAILED = 'failed'             #transaction failed (aborted, invalid, or protocol violation)

class ConcurrencyResponse:
    def __init__(self, transaction_id, query_allowed, reason, status, blocked_by=None, active_transactions=None):
        self.transaction_id = transaction_id
        self.query_allowed = query_allowed
        self.reason = reason
        self.status = status
        self.blocked_by = blocked_by or []
        self.active_transactions = active_transactions or []
    
    @property
    def should_retry(self) -> bool:
        return self.status == LockStatus.WAITING
    
    @property
    def should_rollback(self) -> bool:
        return self.status == LockStatus.FAILED
    
    @property
    def can_proceed(self) -> bool:
        return self.status == LockStatus.GRANTED
