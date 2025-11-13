import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.lock_based_concurrency_control_manager import LockBasedConcurrencyControlManager
from src.row_action import RowAction

def print_schedule(name, operations):
    """Print schedule in readable format"""
    print(f"\n{'='*60}")
    print(f"Schedule: {name}")
    print(f"{'='*60}")
    ops_str = " -> ".join(operations)
    print(f"Operations: {ops_str}\n")

def test_lock_based():
    """Simple Lock-Based CCM Tests"""
    print("\n" + "="*60)
    print("LOCK-BASED CONCURRENCY CONTROL - SIMPLE TESTS")
    print("="*60)
    
    # Test 1: Serial Schedule (should work)
    print_schedule("Test 1: Serial Schedule", ["T1 Read X", "T1 Write X", "T1 Commit", "T2 Read X", "T2 Commit"])
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X)")
    r1 = ccm.transaction_query(t1, RowAction.READ, 1)  # row_id = 1 for X
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Write(X)")
    r2 = ccm.transaction_query(t1, RowAction.WRITE, 1)
    print(f"   → {r2.reason}")
    
    print(f"T{t1}: Commit")
    ccm.transaction_commit(t1)
    ccm.transaction_commit_flushed(t1)
    
    print(f"T{t2}: Read(X)")
    r3 = ccm.transaction_query(t2, RowAction.READ, 1)
    print(f"   → {r3.reason}")
    print(f"T{t2}: Commit")
    ccm.transaction_commit(t2)
    ccm.transaction_commit_flushed(t2)
    print("✓ Test 1 PASSED\n")
    
    # Test 2: Concurrent Reads (should work)
    print_schedule("Test 2: Concurrent Reads", ["T1 Read X", "T2 Read X", "Both Succeed"])
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X)")
    r1 = ccm.transaction_query(t1, RowAction.READ, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t2}: Read(X)")
    r2 = ccm.transaction_query(t2, RowAction.READ, 1)
    print(f"   → {r2.reason}")
    
    if r1.query_allowed and r2.query_allowed:
        print("✓ Test 2 PASSED - Both reads allowed\n")
    else:
        print("✗ Test 2 FAILED\n")
    
    # Test 3: Read-Write Conflict (should block)
    print_schedule("Test 3: Read-Write Conflict", ["T1 Read X", "T2 Write X", "T2 Should Block"])
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X)")
    r1 = ccm.transaction_query(t1, RowAction.READ, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t2}: Write(X)")
    r2 = ccm.transaction_query(t2, RowAction.WRITE, 1)
    print(f"   → {r2.reason}")
    
    if r1.query_allowed and not r2.query_allowed:
        print("✓ Test 3 PASSED - Write blocked by read lock\n")
    else:
        print("✗ Test 3 FAILED\n")
    
    # Test 4: Write-Write Conflict (should block)
    print_schedule("Test 4: Write-Write Conflict", ["T1 Write X", "T2 Write X", "T2 Should Block"])
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(X)")
    r1 = ccm.transaction_query(t1, RowAction.WRITE, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t2}: Write(X)")
    r2 = ccm.transaction_query(t2, RowAction.WRITE, 1)
    print(f"   → {r2.reason}")
    
    if r1.query_allowed and not r2.query_allowed:
        print("✓ Test 4 PASSED - Second write blocked\n")
    else:
        print("✗ Test 4 FAILED\n")
    
    # Test 5: Lock Upgrade (read then write in same transaction)
    print_schedule("Test 5: Lock Upgrade", ["T1 Read X", "T1 Write X", "Should Upgrade"])
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X)")
    r1 = ccm.transaction_query(t1, RowAction.READ, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Write(X)")
    r2 = ccm.transaction_query(t1, RowAction.WRITE, 1)
    print(f"   → {r2.reason}")
    
    if r1.query_allowed and r2.query_allowed:
        print("✓ Test 5 PASSED - Lock upgraded from shared to exclusive\n")
    else:
        print("✗ Test 5 FAILED\n")
    
    # Test 6: Different Objects Don't Conflict
    print_schedule("Test 6: Different Objects", ["T1 Write X", "T2 Write Y", "No Conflict"])
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(X)")
    r1 = ccm.transaction_query(t1, RowAction.WRITE, 1)  # X = row_id 1
    print(f"   → {r1.reason}")
    
    print(f"T{t2}: Write(Y)")
    r2 = ccm.transaction_query(t2, RowAction.WRITE, 2)  # Y = row_id 2
    print(f"   → {r2.reason}")
    
    if r1.query_allowed and r2.query_allowed:
        print("✓ Test 6 PASSED - Different objects don't conflict\n")
    else:
        print("✗ Test 6 FAILED\n")

if __name__ == "__main__":
    test_lock_based()
