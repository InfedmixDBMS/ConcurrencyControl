from .row_action import TableAction
from .concurrency_response import ConcurrencyResponse, LockStatus
from .concurrency_control_manager import ConcurrencyControlManager

class LockBasedConcurrencyControlManager(ConcurrencyControlManager):

    def __init__(self):
        super().__init__()
        self.shared_locks = {}
        self.exclusive_locks = {}
        self.wait_queue = {}
        self.timestamp_counter = 0

    def transaction_begin(self) -> int:
        transaction_id = super().transaction_begin()
        self.timestamp_counter += 1
        self.transactions[transaction_id] = {
            **self.transactions[transaction_id],
            'shared_tables': set(),
            'exclusive_tables': set(),
            'timestamp': self.timestamp_counter,
            'has_released_lock': False,
            'waiting_for': None 
        }
        return transaction_id

    def __transaction_release_locks(self, transaction_id: int) -> None:
        transaction = self.transactions[transaction_id]
        transaction['has_released_lock'] = True  #entering shrinking phase
        
        #release locks
        for table_name in transaction['shared_tables']:
            shared_holders = self.shared_locks.get(table_name)
            if shared_holders is None:
                continue
            shared_holders.discard(transaction_id)
            if len(shared_holders) == 0:
                del self.shared_locks[table_name]
        
        for table_name in transaction['exclusive_tables']:
            exclusive_holder = self.exclusive_locks.get(table_name)
            if exclusive_holder != transaction_id:
                continue
            del self.exclusive_locks[table_name]
        
        self.__process_wait_queue()

    def transaction_commit_flushed(self, transaction_id: int) -> None:
        super().transaction_commit_flushed(transaction_id)
        self.__transaction_release_locks(transaction_id)

    def transaction_abort(self, transaction_id: int) -> None:
        super().transaction_abort(transaction_id)
        self.__transaction_release_locks(transaction_id)
    
    def __process_wait_queue(self):
        """Process wait queue after locks are released"""
        
        # TODO: Kasih sinyal ke komponen lain "transaksinya udh selese, yg nunggu bisa lanjut"
        
        # Sementara cm dikosongin aja queue-nya
        for tid in self.transactions:
            if self.transactions[tid].get('waiting_for'):
                self.transactions[tid]['waiting_for'] = None
    
    def __get_lock_holder(self, table_name: str, action: TableAction):
        """Get current lock holder(s) for a table"""
        exclusive_holder = self.exclusive_locks.get(table_name)
        shared_holders = self.shared_locks.get(table_name)
        
        if action == TableAction.READ:
            return exclusive_holder, None
        else: 
            return exclusive_holder, shared_holders
    
    def __wait_die_check(self, requesting_txn_id: int, holder_txn_id: int) -> tuple[bool, str]:
        """
        Wait-Die protocol (non-preemptive)
        - If requesting is OLDER: it WAITS
        - If requesting is YOUNGER: it DIES (aborts)
        Returns: (should_abort_requester, reason)
        """
        requesting_ts = self.transactions[requesting_txn_id]['timestamp']
        holder_ts = self.transactions[holder_txn_id]['timestamp']
        
        if requesting_ts < holder_ts:
            # wait if requester is older
            return False, f'Transaction {requesting_txn_id} (TS={requesting_ts}) waits for younger holder {holder_txn_id} (TS={holder_ts})'
        else:
            # kill if requester is younger
            return True, f'Transaction {requesting_txn_id} (TS={requesting_ts}) aborted by Wait-Die (younger than holder {holder_txn_id} TS={holder_ts})'

    def transaction_query(self, transaction_id: int, table_action: TableAction, table_name: str) -> ConcurrencyResponse:
        """
        Request lock on a table with 2PL and Wait-Die protocol.
        """
        #check if transaction exists
        if transaction_id not in self.transactions:
            return ConcurrencyResponse(
                transaction_id,
                False,
                f'transaction {transaction_id} does not exist',
                LockStatus.FAILED
            )
        
        transaction = self.transactions[transaction_id]
        
        #check if transaction is in queryable state
        if transaction['status'].value not in ['active']:
            return ConcurrencyResponse(
                transaction_id,
                False,
                f'transaction {transaction_id} is in {transaction["status"].value} state',
                LockStatus.FAILED
            )
        
        #check 2pl violation
        if transaction['has_released_lock']:
            return ConcurrencyResponse(
                transaction_id, 
                False, 
                f'transaction {transaction_id} violated 2pl',
                LockStatus.FAILED
            )
        
        shared_holders = self.shared_locks.get(table_name)
        exclusive_holder = self.exclusive_locks.get(table_name)
        
        if table_action == TableAction.READ:
            if exclusive_holder is not None:
                if exclusive_holder != transaction_id:
                    should_abort, reason = self.__wait_die_check(transaction_id, exclusive_holder)
                    
                    if should_abort:
                        super().transaction_rollback(transaction_id)
                        return ConcurrencyResponse(
                            transaction_id, 
                            False, 
                            f'Read denied (Wait-Die): {reason}',
                            LockStatus.FAILED,
                            blocked_by=[exclusive_holder]
                        )
                    else:
                        transaction['waiting_for'] = exclusive_holder
                        return ConcurrencyResponse(
                            transaction_id, 
                            False, 
                            f'Read waiting (Wait-Die): {reason}',
                            LockStatus.WAITING,
                            blocked_by=[exclusive_holder]
                        )
            else:
                if shared_holders is None:
                    shared_holders = set()
                    self.shared_locks[table_name] = shared_holders
                transaction['shared_tables'].add(table_name)
                shared_holders.add(transaction_id)
            return ConcurrencyResponse(
                transaction_id, 
                True, 
                f'Read lock granted on table {table_name}',
                LockStatus.GRANTED
            )
        
        if table_action == TableAction.WRITE:
            if exclusive_holder == transaction_id:
                return ConcurrencyResponse(
                    transaction_id, 
                    True, 
                    f'Write lock already held on table {table_name}',
                    LockStatus.GRANTED
                )
            
            if exclusive_holder is not None:
                should_abort, reason = self.__wait_die_check(transaction_id, exclusive_holder)
                
                if should_abort:
                    super().transaction_rollback(transaction_id)
                    return ConcurrencyResponse(
                        transaction_id, 
                        False, 
                        f'Write denied (Wait-Die): {reason}',
                        LockStatus.FAILED,
                        blocked_by=[exclusive_holder]
                    )
                else:
                    transaction['waiting_for'] = exclusive_holder
                    return ConcurrencyResponse(
                        transaction_id, 
                        False, 
                        f'Write waiting (Wait-Die): {reason}',
                        LockStatus.WAITING,
                        blocked_by=[exclusive_holder]
                    )
            
            if shared_holders is not None:
                other_shared_holders = shared_holders - {transaction_id}
                if len(other_shared_holders) > 0:
                    first_holder = next(iter(other_shared_holders))
                    should_abort, reason = self.__wait_die_check(transaction_id, first_holder)
                    
                    if should_abort:
                        super().transaction_rollback(transaction_id)
                        return ConcurrencyResponse(
                            transaction_id, 
                            False, 
                            f'Write denied (Wait-Die): {reason}',
                            LockStatus.FAILED,
                            blocked_by=list(other_shared_holders)
                        )
                    else:
                        transaction['waiting_for'] = first_holder
                        return ConcurrencyResponse(
                            transaction_id, 
                            False, 
                            f'Write waiting (Wait-Die): shared locks held by {len(other_shared_holders)} transaction(s)',
                            LockStatus.WAITING,
                            blocked_by=list(other_shared_holders)
                        )
                
                #only this transaction holds shared lock so upgrade to exclusive
                shared_holders.discard(transaction_id)
                transaction['shared_tables'].discard(table_name)
                if len(shared_holders) == 0:
                    del self.shared_locks[table_name]
            
            transaction['exclusive_tables'].add(table_name)
            self.exclusive_locks[table_name] = transaction_id
            return ConcurrencyResponse(
                transaction_id, 
                True, 
                f'Write lock granted on table {table_name} (exclusive)',
                LockStatus.GRANTED
            )
        
        raise Exception(f'Unknown table action {table_action}')
