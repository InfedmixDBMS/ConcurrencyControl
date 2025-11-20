"""
Demonstration of ConcurrencyResponse API for Query Processor Integration

This shows how to use the enhanced response object with LockStatus enum.
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.lock_based_concurrency_control_manager import LockBasedConcurrencyControlManager
from src.row_action import TableAction

def demo_response_api():
    print("="*70)
    print("CONCURRENCY RESPONSE API DEMO")
    print("="*70)
    
    ccm = LockBasedConcurrencyControlManager()
    
    #scenario 1: granted
    print("\n1. GRANTED - Lock acquired successfully")
    print("-" * 70)
    t1 = ccm.transaction_begin()
    response = ccm.transaction_query(t1, TableAction.WRITE, 'X')
    print(f"Response: {response.reason}")
    print(f"Status: {response.status.value}")
    print(f"Can proceed: {response.can_proceed}")
    print(f"Should retry: {response.should_retry}")
    print(f"Should rollback: {response.should_rollback}")
    
    #scenario 2: waiting
    print("\n2. WAITING - Older transaction waits for younger")
    print("-" * 70)
    t2 = ccm.transaction_begin()
    ccm.transaction_query(t2, TableAction.WRITE, 'Y')
    response = ccm.transaction_query(t1, TableAction.WRITE, 'Y')  #t1 is older
    print(f"Response: {response.reason}")
    print(f"Status: {response.status.value}")
    print(f"Can proceed: {response.can_proceed}")
    print(f"Should retry: {response.should_retry}")  #true
    print(f"Should rollback: {response.should_rollback}")
    
    #scenario 3: failed (aborted by wait-die)
    print("\n3. FAILED - Younger transaction killed by Wait-Die")
    print("-" * 70)
    t3 = ccm.transaction_begin()
    response = ccm.transaction_query(t3, TableAction.WRITE, 'X')  #t3 is younger than t1
    print(f"Response: {response.reason}")
    print(f"Status: {response.status.value}")
    print(f"Can proceed: {response.can_proceed}")
    print(f"Should retry: {response.should_retry}")
    print(f"Should rollback: {response.should_rollback}")  #true
    
    #scenario 4: failed (2pl violation)
    print("\n4. FAILED - Violated two-phase locking")
    print("-" * 70)
    t4 = ccm.transaction_begin()
    ccm.transaction_query(t4, TableAction.WRITE, 'Z')
    ccm.transaction_commit(t4)
    ccm.transaction_commit_flushed(t4)
    response = ccm.transaction_query(t4, TableAction.WRITE, 'W')
    print(f"Response: {response.reason}")
    print(f"Status: {response.status.value}")
    print(f"Can proceed: {response.can_proceed}")
    print(f"Should retry: {response.should_retry}")
    print(f"Should rollback: {response.should_rollback}")  #true
    
    #scenario 5: failed (invalid transaction)
    print("\n5. FAILED - Transaction doesn't exist")
    print("-" * 70)
    response = ccm.transaction_query(999, TableAction.READ, 'A')
    print(f"Response: {response.reason}")
    print(f"Status: {response.status.value}")
    print(f"Can proceed: {response.can_proceed}")
    print(f"Should retry: {response.should_retry}")
    print(f"Should rollback: {response.should_rollback}")  #true

if __name__ == '__main__':
    demo_response_api()
