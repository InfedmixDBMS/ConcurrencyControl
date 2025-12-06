import threading
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
        
        # Event-driven wake-up mechanism
        self.resource_waiters = {}
        self.events_lock = threading.Lock()
        
        # Wait-for graph for deadlock detection
        # wait_for_graph[T1] = {T2, T3} means T1 is waiting for T2 and T3
        self.wait_for_graph = {}
        self.wait_for_lock = threading.Lock()

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
        
        # No need to create event in advance - created on demand when waiting
        
        return transaction_id
    
    def _detect_deadlock(self, transaction_id: int) -> bool:
        """
        Detect if adding transaction_id to wait-for graph would create a cycle.
        Uses DFS to detect cycles in the wait-for graph.
        
        Returns:
            True if deadlock detected (cycle exists), False otherwise
        """
        visited = set()
        rec_stack = set()
        
        def has_cycle(node: int) -> bool:
            """DFS to detect cycle starting from node"""
            if node in rec_stack:
                return True  # Back edge found - cycle detected
            if node in visited:
                return False  # Already explored this path
            
            visited.add(node)
            rec_stack.add(node)
            
            # Check all nodes this transaction is waiting for
            waiting_for = self.wait_for_graph.get(node, set())
            for neighbor in waiting_for:
                if has_cycle(neighbor):
                    return True
            
            rec_stack.remove(node)
            return False
        
        with self.wait_for_lock:
            # Start DFS from the requesting transaction
            return has_cycle(transaction_id)
    
    def _add_to_wait_for_graph(self, waiter: int, holders: set):
        """Add edges to wait-for graph: waiter -> each holder"""
        with self.wait_for_lock:
            if waiter not in self.wait_for_graph:
                self.wait_for_graph[waiter] = set()
            self.wait_for_graph[waiter].update(holders)
    
    def _clear_wait_event(self, transaction_id: int, resource_name: str):
        """Clear wait event for a transaction that successfully acquired a lock"""
        with self.events_lock:
            if resource_name in self.resource_waiters:
                if transaction_id in self.resource_waiters[resource_name]:
                    del self.resource_waiters[resource_name][transaction_id]
                    # Clean up empty resource entries
                    if len(self.resource_waiters[resource_name]) == 0:
                        del self.resource_waiters[resource_name]

    def _remove_from_wait_for_graph(self, transaction_id: int):
        """Remove transaction from wait-for graph (both as waiter and holder)"""
        with self.wait_for_lock:
            # Remove as waiter
            if transaction_id in self.wait_for_graph:
                del self.wait_for_graph[transaction_id]
            
            # Remove as holder (remove edges pointing to this transaction)
            for waiter in list(self.wait_for_graph.keys()):
                self.wait_for_graph[waiter].discard(transaction_id)
                # Clean up empty entries
                if not self.wait_for_graph[waiter]:
                    del self.wait_for_graph[waiter]

    def __transaction_release_locks(self, transaction_id: int) -> None:
        transaction = self.transactions[transaction_id]
        transaction['has_released_lock'] = True  #entering shrinking phase
        
        # Remove from wait-for graph
        self._remove_from_wait_for_graph(transaction_id)
        
        #track which resources are being freed
        freed_resources = set()
        
        #release locks
        for table_name in transaction['shared_tables']:
            shared_holders = self.shared_locks.get(table_name)
            if shared_holders is None:
                continue
            shared_holders.discard(transaction_id)
            if len(shared_holders) == 0:
                del self.shared_locks[table_name]
                freed_resources.add(table_name)  #completely freed
            else:
                freed_resources.add(table_name)  #reduced holders, waiters might proceed
        
        for table_name in transaction['exclusive_tables']:
            exclusive_holder = self.exclusive_locks.get(table_name)
            if exclusive_holder != transaction_id:
                continue
            del self.exclusive_locks[table_name]
            freed_resources.add(table_name)  #completely freed
        
        self.__process_wait_queue(freed_resources)

    def transaction_commit_flushed(self, transaction_id: int) -> None:
        super().transaction_commit_flushed(transaction_id)
        self.__transaction_release_locks(transaction_id)

    def transaction_rollback(self, transaction_id: int) -> None:
        super().transaction_rollback(transaction_id)
        self.__transaction_release_locks(transaction_id)

    def transaction_abort(self, transaction_id: int) -> None:
        super().transaction_abort(transaction_id)
        self.__transaction_release_locks(transaction_id)
    
    def transaction_end(self, transaction_id: int) -> None:
        """Override to cleanup events"""
        super().transaction_end(transaction_id)
        
        # Cleanup: remove transaction from all resource waiters
        with self.events_lock:
            for resource_name in list(self.resource_waiters.keys()):
                if transaction_id in self.resource_waiters[resource_name]:
                    del self.resource_waiters[resource_name][transaction_id]
                    # Clean up empty resource entries
                    if len(self.resource_waiters[resource_name]) == 0:
                        del self.resource_waiters[resource_name]
    
    def get_wait_event(self, transaction_id: int) -> threading.Event:
        """Get the event object for a waiting transaction"""
        with self.events_lock:
            # Find which resource this transaction is waiting for
            for resource_name, waiters in self.resource_waiters.items():
                if transaction_id in waiters:
                    return waiters[transaction_id]
            # If not found, return None (shouldn't happen in normal flow)
            return None
    
    def register_waiting_transaction(self, transaction_id: int, resource_name: str):
        """Register a transaction as waiting for a specific resource"""
        with self.events_lock:
            if resource_name not in self.resource_waiters:
                self.resource_waiters[resource_name] = {}
            
            # Create or reuse event for this transaction on this resource
            if transaction_id not in self.resource_waiters[resource_name]:
                self.resource_waiters[resource_name][transaction_id] = threading.Event()
            else:
                # Clear the event if reusing (shouldn't happen normally)
                self.resource_waiters[resource_name][transaction_id].clear()
    
    def __process_wait_queue(self, freed_resources: set):
        """Process wait queue after locks are released - signal waiting transactions"""
        
        #signal all transactions waiting on freed resources
        with self.events_lock:
            for resource_name in freed_resources:
                if resource_name in self.resource_waiters:
                    #signal all waiters for this resource
                    for tid, event in self.resource_waiters[resource_name].items():
                        event.set()  #wake up the waiting transaction
                    # DON'T delete waiters here - let them clean up after acquiring lock
        
        #clear waiting_for flags
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
    
    def __get_active_transactions(self) -> list[int]:
        """Get list of active transaction IDs"""
        from .transaction_status import TransactionStatus
        return [tid for tid, txn in self.transactions.items() if txn['status'] == TransactionStatus.ACTIVE]

    def transaction_query(self, transaction_id: int, table_action: TableAction, table_name: str) -> ConcurrencyResponse:
        """
        Request lock on a table with 2PL and deadlock detection.
        Transactions wait when lock conflicts occur.
        Deadlock is detected and one transaction is aborted to break the cycle.
        """
        #check if transaction exists
        if transaction_id not in self.transactions:
            return ConcurrencyResponse(
                transaction_id,
                f'transaction {transaction_id} does not exist',
                LockStatus.FAILED,
                blocked_by=[],
                active_transactions=self.__get_active_transactions()
            )
        
        transaction = self.transactions[transaction_id]
        
        #check if transaction is in queryable state
        if transaction['status'].value not in ['active']:
            return ConcurrencyResponse(
                transaction_id,
                f'transaction {transaction_id} is in {transaction["status"].value} state',
                LockStatus.FAILED,
                blocked_by=[],
                active_transactions=self.__get_active_transactions()
            )
        
        #check 2pl violation
        if transaction['has_released_lock']:
            return ConcurrencyResponse(
                transaction_id, 
                f'transaction {transaction_id} violated 2pl',
                LockStatus.FAILED,
                blocked_by=[],
                active_transactions=self.__get_active_transactions()
            )
        
        shared_holders = self.shared_locks.get(table_name)
        exclusive_holder = self.exclusive_locks.get(table_name)
        
        if table_action == TableAction.READ:
            if exclusive_holder is not None:
                if exclusive_holder != transaction_id:
                    # Lock conflict - add to wait-for graph and check for deadlock
                    self._add_to_wait_for_graph(transaction_id, {exclusive_holder})
                    
                    if self._detect_deadlock(transaction_id):
                        # Deadlock detected - abort this transaction (victim)
                        self._remove_from_wait_for_graph(transaction_id)
                        super().transaction_rollback(transaction_id)
                        self.__transaction_release_locks(transaction_id)
                        return ConcurrencyResponse(
                            transaction_id, 
                            f'Deadlock detected. Transaction {transaction_id} aborted (victim selection).',
                            LockStatus.FAILED,
                            blocked_by=[exclusive_holder],
                            active_transactions=self.__get_active_transactions()
                        )
                    
                    # No deadlock - safe to wait
                    transaction['waiting_for'] = exclusive_holder
                    # Register this transaction as waiting for the resource
                    self.register_waiting_transaction(transaction_id, table_name)
                    return ConcurrencyResponse(
                        transaction_id, 
                        f'Read waiting for exclusive lock holder {exclusive_holder}',
                        LockStatus.WAITING,
                        blocked_by=[exclusive_holder],
                        active_transactions=self.__get_active_transactions()
                    )
            else:
                # No exclusive lock - grant shared lock
                if shared_holders is None:
                    shared_holders = set()
                    self.shared_locks[table_name] = shared_holders
                transaction['shared_tables'].add(table_name)
                shared_holders.add(transaction_id)
                # Clear wait event if this transaction was waiting
                self._clear_wait_event(transaction_id, table_name)
            
            return ConcurrencyResponse(
                transaction_id, 
                f'Read lock granted on table {table_name}',
                LockStatus.GRANTED,
                blocked_by=[],
                active_transactions=self.__get_active_transactions()
            )
        
        if table_action == TableAction.WRITE:
            # Already holds exclusive lock
            if exclusive_holder == transaction_id:
                return ConcurrencyResponse(
                    transaction_id, 
                    f'Write lock already held on table {table_name}',
                    LockStatus.GRANTED,
                    blocked_by=[],
                    active_transactions=self.__get_active_transactions()
                )
            
            # Exclusive lock held by another transaction
            if exclusive_holder is not None:
                # Add to wait-for graph and check for deadlock
                self._add_to_wait_for_graph(transaction_id, {exclusive_holder})
                
                if self._detect_deadlock(transaction_id):
                    # Deadlock detected - abort this transaction
                    self._remove_from_wait_for_graph(transaction_id)
                    super().transaction_rollback(transaction_id)
                    self.__transaction_release_locks(transaction_id)
                    return ConcurrencyResponse(
                        transaction_id, 
                        f'Deadlock detected. Transaction {transaction_id} aborted (victim selection).',
                        LockStatus.FAILED,
                        blocked_by=[exclusive_holder],
                        active_transactions=self.__get_active_transactions()
                    )
                
                # No deadlock - safe to wait
                transaction['waiting_for'] = exclusive_holder
                # Register this transaction as waiting for the resource
                self.register_waiting_transaction(transaction_id, table_name)
                return ConcurrencyResponse(
                    transaction_id, 
                    f'Write waiting for exclusive lock holder {exclusive_holder}',
                    LockStatus.WAITING,
                    blocked_by=[exclusive_holder],
                    active_transactions=self.__get_active_transactions()
                )
            
            # Shared locks held by other transactions
            if shared_holders is not None:
                other_shared_holders = shared_holders - {transaction_id}
                if len(other_shared_holders) > 0:
                    # Add to wait-for graph and check for deadlock
                    self._add_to_wait_for_graph(transaction_id, other_shared_holders)
                    
                    if self._detect_deadlock(transaction_id):
                        # Deadlock detected - abort this transaction
                        self._remove_from_wait_for_graph(transaction_id)
                        super().transaction_rollback(transaction_id)
                        self.__transaction_release_locks(transaction_id)
                        return ConcurrencyResponse(
                            transaction_id, 
                            f'Deadlock detected. Transaction {transaction_id} aborted (victim selection).',
                            LockStatus.FAILED,
                            blocked_by=list(other_shared_holders),
                            active_transactions=self.__get_active_transactions()
                        )
                    
                    # No deadlock - safe to wait
                    first_holder = next(iter(other_shared_holders))
                    transaction['waiting_for'] = first_holder
                    # Register this transaction as waiting for the resource
                    self.register_waiting_transaction(transaction_id, table_name)
                    return ConcurrencyResponse(
                        transaction_id, 
                        f'Write waiting for shared locks held by {len(other_shared_holders)} transaction(s)',
                        LockStatus.WAITING,
                        blocked_by=list(other_shared_holders),
                        active_transactions=self.__get_active_transactions()
                    )
                
                # Only this transaction holds shared lock - upgrade to exclusive
                shared_holders.discard(transaction_id)
                transaction['shared_tables'].discard(table_name)
                if len(shared_holders) == 0:
                    del self.shared_locks[table_name]
            
            # Grant exclusive lock
            transaction['exclusive_tables'].add(table_name)
            self.exclusive_locks[table_name] = transaction_id
            # Clear wait event if this transaction was waiting
            self._clear_wait_event(transaction_id, table_name)
            return ConcurrencyResponse(
                transaction_id, 
                f'Write lock granted on table {table_name} (exclusive)',
                LockStatus.GRANTED,
                blocked_by=[],
                active_transactions=self.__get_active_transactions()
            )
        
        raise Exception(f'Unknown table action {table_action}')