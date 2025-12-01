"""
Test to verify blocked_by field is correctly populated in ConcurrencyResponse
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.lock_based_concurrency_control_manager import LockBasedConcurrencyControlManager
from src.row_action import TableAction
from src.concurrency_response import LockStatus

def test_blocked_by_field():
    print("\n" + "="*70)
    print("BLOCKED_BY FIELD TESTS")
    print("="*70)
    
    #test 1: single exclusive lock blocker
    print("\n" + "-"*70)
    print("Test 1: Single Exclusive Lock Blocker")
    print("-"*70)
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(X)")
    r1 = ccm.transaction_query(t1, TableAction.WRITE, 'X')
    print(f"  Status: {r1.status.value}")
    print(f"  Blocked by: {r1.blocked_by}")
    
    print(f"T{t2}: Write(X) - should be blocked by T{t1}")
    r2 = ccm.transaction_query(t2, TableAction.WRITE, 'X')
    print(f"  Status: {r2.status.value}")
    print(f"  Blocked by: {r2.blocked_by}")
    
    if r2.status == LockStatus.FAILED and r2.blocked_by == [t1]:
        print("✓ PASSED - blocked_by correctly contains [T1]")
    else:
        print(f"✗ FAILED - Expected blocked_by=[{t1}], got {r2.blocked_by}")
    
    #test 2: older transaction waits
    print("\n" + "-"*70)
    print("Test 2: Older Transaction Waits (WAITING status)")
    print("-"*70)
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()  #t1 has timestamp 1
    t2 = ccm.transaction_begin()  #t2 has timestamp 2
    
    print(f"T{t2}: Write(Y)")
    r1 = ccm.transaction_query(t2, TableAction.WRITE, 'Y')
    print(f"  Status: {r1.status.value}")
    
    print(f"T{t1}: Write(Y) - T{t1} is older (TS={ccm.transactions[t1]['timestamp']}), should WAIT for T{t2} (TS={ccm.transactions[t2]['timestamp']})")
    r2 = ccm.transaction_query(t1, TableAction.WRITE, 'Y')
    print(f"  Status: {r2.status.value}")
    print(f"  Blocked by: {r2.blocked_by}")
    
    if r2.status == LockStatus.WAITING and r2.blocked_by == [t2]:
        print(f"✓ PASSED - blocked_by correctly contains [T{t2}]")
    else:
        print(f"✗ FAILED - Expected WAITING with blocked_by=[{t2}], got {r2.status.value} with {r2.blocked_by}")
    
    #test 3: multiple shared lock holders
    print("\n" + "-"*70)
    print("Test 3: Multiple Shared Lock Holders")
    print("-"*70)
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    t3 = ccm.transaction_begin()
    t4 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(Z)")
    ccm.transaction_query(t1, TableAction.READ, 'Z')
    print(f"T{t2}: Read(Z)")
    ccm.transaction_query(t2, TableAction.READ, 'Z')
    print(f"T{t3}: Read(Z)")
    ccm.transaction_query(t3, TableAction.READ, 'Z')
    
    print(f"T{t4}: Write(Z) - should be blocked by T{t1}, T{t2}, T{t3}")
    r4 = ccm.transaction_query(t4, TableAction.WRITE, 'Z')
    print(f"  Status: {r4.status.value}")
    print(f"  Blocked by: {r4.blocked_by}")
    
    #check that blocked_by contains all three readers
    if r4.status == LockStatus.FAILED and set(r4.blocked_by) == {t1, t2, t3}:
        print(f"✓ PASSED - blocked_by correctly contains all readers: {r4.blocked_by}")
    else:
        print(f"✗ FAILED - Expected blocked_by to contain [{t1}, {t2}, {t3}], got {r4.blocked_by}")
    
    #test 4: read blocked by exclusive lock
    print("\n" + "-"*70)
    print("Test 4: Read Blocked by Exclusive Lock")
    print("-"*70)
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(A)")
    ccm.transaction_query(t1, TableAction.WRITE, 'A')
    
    print(f"T{t2}: Read(A) - should be blocked by T{t1}")
    r2 = ccm.transaction_query(t2, TableAction.READ, 'A')
    print(f"  Status: {r2.status.value}")
    print(f"  Blocked by: {r2.blocked_by}")
    
    if r2.status == LockStatus.FAILED and r2.blocked_by == [t1]:
        print(f"✓ PASSED - blocked_by correctly contains [T{t1}]")
    else:
        print(f"✗ FAILED - Expected blocked_by=[{t1}], got {r2.blocked_by}")
    
    #test 5: no blocker (granted)
    print("\n" + "-"*70)
    print("Test 5: No Blocker (GRANTED status)")
    print("-"*70)
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(B)")
    r1 = ccm.transaction_query(t1, TableAction.WRITE, 'B')
    print(f"  Status: {r1.status.value}")
    print(f"  Blocked by: {r1.blocked_by}")
    
    if r1.status == LockStatus.GRANTED and r1.blocked_by == []:
        print("✓ PASSED - blocked_by is empty for granted lock")
    else:
        print(f"✗ FAILED - Expected blocked_by=[], got {r1.blocked_by}")
    
    #test 6: lock upgrade waiting on other readers
    print("\n" + "-"*70)
    print("Test 6: Lock Upgrade Waiting on Other Readers")
    print("-"*70)
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    t3 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(C)")
    ccm.transaction_query(t1, TableAction.READ, 'C')
    print(f"T{t2}: Read(C)")
    ccm.transaction_query(t2, TableAction.READ, 'C')
    
    print(f"T{t1}: Write(C) - upgrade, but T{t2} holds shared lock")
    r1 = ccm.transaction_query(t1, TableAction.WRITE, 'C')
    print(f"  Status: {r1.status.value}")
    print(f"  Blocked by: {r1.blocked_by}")
    
    if r1.status == LockStatus.WAITING and r1.blocked_by == [t2]:
        print(f"✓ PASSED - blocked_by correctly contains [T{t2}]")
    else:
        print(f"✗ FAILED - Expected blocked_by=[{t2}], got {r1.blocked_by}")
    
    print("\n" + "="*70)
    print("BLOCKED_BY FIELD TESTS COMPLETE")
    print("="*70)

if __name__ == "__main__":
    test_blocked_by_field()
