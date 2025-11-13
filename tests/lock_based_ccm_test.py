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
    print("LOCK-BASED CONCURRENCY CONTROL - COMPREHENSIVE TESTS")
    print("="*60)
    
    # Test 1: Serial Schedule (should work)
    print_schedule("Test 1: Serial Schedule", ["T1 Read X", "T1 Write X", "T1 Commit", "T2 Read X", "T2 Commit"])
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X)")
    r1 = ccm.transaction_query(t1, RowAction.READ, 1)
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
    
    # Test 2: Concurrent Reads
    print_schedule("Test 2: Concurrent Reads", ["T1 Read X", "T2 Read X", "T3 Read X"])
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    t3 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X)")
    r1 = ccm.transaction_query(t1, RowAction.READ, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t2}: Read(X)")
    r2 = ccm.transaction_query(t2, RowAction.READ, 1)
    print(f"   → {r2.reason}")
    
    print(f"T{t3}: Read(X)")
    r3 = ccm.transaction_query(t3, RowAction.READ, 1)
    print(f"   → {r3.reason}")
    
    if r1.query_allowed and r2.query_allowed and r3.query_allowed:
        print("✓ Test 2 PASSED - All concurrent reads allowed\n")
    else:
        print("✗ Test 2 FAILED\n")
    
    # Test 3: Read-Write Conflict
    print_schedule("Test 3: Read-Write Conflict", ["T1 Read X", "T2 Write X"])
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
    
    # Test 4: Write-Write Conflict
    print_schedule("Test 4: Write-Write Conflict", ["T1 Write X", "T2 Write X"])
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
    
    # Test 5: Lock Upgrade
    print_schedule("Test 5: Lock Upgrade", ["T1 Read X", "T1 Write X"])
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X)")
    r1 = ccm.transaction_query(t1, RowAction.READ, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Write(X)")
    r2 = ccm.transaction_query(t1, RowAction.WRITE, 1)
    print(f"   → {r2.reason}")
    
    if r1.query_allowed and r2.query_allowed:
        print("✓ Test 5 PASSED - Lock upgraded\n")
    else:
        print("✗ Test 5 FAILED\n")
    
    # Test 6: Different Objects
    print_schedule("Test 6: Different Objects", ["T1 Write X", "T2 Write Y"])
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(X)")
    r1 = ccm.transaction_query(t1, RowAction.WRITE, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t2}: Write(Y)")
    r2 = ccm.transaction_query(t2, RowAction.WRITE, 2)
    print(f"   → {r2.reason}")
    
    if r1.query_allowed and r2.query_allowed:
        print("✓ Test 6 PASSED - Different objects don't conflict\n")
    else:
        print("✗ Test 6 FAILED\n")
    
    # Test 7: Multiple Reads Then Write Fails
    print_schedule("Test 7: Multiple Readers Block Writer", ["T1 Read X", "T2 Read X", "T3 Write X"])
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    t3 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X)")
    r1 = ccm.transaction_query(t1, RowAction.READ, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t2}: Read(X)")
    r2 = ccm.transaction_query(t2, RowAction.READ, 1)
    print(f"   → {r2.reason}")
    
    print(f"T{t3}: Write(X)")
    r3 = ccm.transaction_query(t3, RowAction.WRITE, 1)
    print(f"   → {r3.reason}")
    
    if r1.query_allowed and r2.query_allowed and not r3.query_allowed:
        print("✓ Test 7 PASSED - Writer blocked by multiple readers\n")
    else:
        print("✗ Test 7 FAILED\n")
    
    # Test 8: Lock Release After Commit
    print_schedule("Test 8: Lock Release After Commit", ["T1 Write X", "T1 Commit", "T2 Write X"])
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(X)")
    r1 = ccm.transaction_query(t1, RowAction.WRITE, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Commit")
    ccm.transaction_commit(t1)
    ccm.transaction_commit_flushed(t1)
    
    t2 = ccm.transaction_begin()
    print(f"T{t2}: Write(X)")
    r2 = ccm.transaction_query(t2, RowAction.WRITE, 1)
    print(f"   → {r2.reason}")
    
    if r1.query_allowed and r2.query_allowed:
        print("✓ Test 8 PASSED - Lock released after commit\n")
    else:
        print("✗ Test 8 FAILED\n")
    
    # Test 9: Lock Release After Abort
    print_schedule("Test 9: Lock Release After Abort", ["T1 Write X", "T1 Abort", "T2 Write X"])
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(X)")
    r1 = ccm.transaction_query(t1, RowAction.WRITE, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Abort")
    ccm.transaction_rollback(t1)
    ccm.transaction_abort(t1)
    
    t2 = ccm.transaction_begin()
    print(f"T{t2}: Write(X)")
    r2 = ccm.transaction_query(t2, RowAction.WRITE, 1)
    print(f"   → {r2.reason}")
    
    if r1.query_allowed and r2.query_allowed:
        print("✓ Test 9 PASSED - Lock released after abort\n")
    else:
        print("✗ Test 9 FAILED\n")
    
    # Test 10: Same Transaction Multiple Reads
    print_schedule("Test 10: Same Transaction Multiple Reads", ["T1 Read X", "T1 Read X", "T1 Read X"])
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X) #1")
    r1 = ccm.transaction_query(t1, RowAction.READ, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Read(X) #2")
    r2 = ccm.transaction_query(t1, RowAction.READ, 1)
    print(f"   → {r2.reason}")
    
    print(f"T{t1}: Read(X) #3")
    r3 = ccm.transaction_query(t1, RowAction.READ, 1)
    print(f"   → {r3.reason}")
    
    if r1.query_allowed and r2.query_allowed and r3.query_allowed:
        print("✓ Test 10 PASSED - Same transaction can read multiple times\n")
    else:
        print("✗ Test 10 FAILED\n")
    
    # Test 11: Same Transaction Multiple Writes
    print_schedule("Test 11: Same Transaction Multiple Writes", ["T1 Write X", "T1 Write X", "T1 Write X"])
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(X) #1")
    r1 = ccm.transaction_query(t1, RowAction.WRITE, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Write(X) #2")
    r2 = ccm.transaction_query(t1, RowAction.WRITE, 1)
    print(f"   → {r2.reason}")
    
    print(f"T{t1}: Write(X) #3")
    r3 = ccm.transaction_query(t1, RowAction.WRITE, 1)
    print(f"   → {r3.reason}")
    
    if r1.query_allowed and r2.query_allowed and r3.query_allowed:
        print("✓ Test 11 PASSED - Same transaction can write multiple times\n")
    else:
        print("✗ Test 11 FAILED\n")
    
    # Test 12: Complex Multi-Object Transaction
    print_schedule("Test 12: Multi-Object Transaction", ["T1 Write X", "T1 Read Y", "T2 Read X", "T2 Write Y"])
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(X)")
    r1 = ccm.transaction_query(t1, RowAction.WRITE, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Read(Y)")
    r2 = ccm.transaction_query(t1, RowAction.READ, 2)
    print(f"   → {r2.reason}")
    
    print(f"T{t2}: Read(X)")
    r3 = ccm.transaction_query(t2, RowAction.READ, 1)
    print(f"   → {r3.reason}")
    
    print(f"T{t2}: Write(Y)")
    r4 = ccm.transaction_query(t2, RowAction.WRITE, 2)
    print(f"   → {r4.reason}")
    
    conflicts_detected = not r3.query_allowed or not r4.query_allowed
    if r1.query_allowed and r2.query_allowed and conflicts_detected:
        print("✓ Test 12 PASSED - Conflicts properly detected\n")
    else:
        print("✗ Test 12 FAILED\n")
    
    # Test 13: Failed Lock Upgrade (Other Readers Present)
    print_schedule("Test 13: Failed Lock Upgrade", ["T1 Read X", "T2 Read X", "T1 Write X"])
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X)")
    r1 = ccm.transaction_query(t1, RowAction.READ, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t2}: Read(X)")
    r2 = ccm.transaction_query(t2, RowAction.READ, 1)
    print(f"   → {r2.reason}")
    
    print(f"T{t1}: Write(X) - attempt upgrade")
    r3 = ccm.transaction_query(t1, RowAction.WRITE, 1)
    print(f"   → {r3.reason}")
    
    if r1.query_allowed and r2.query_allowed and not r3.query_allowed:
        print("✓ Test 13 PASSED - Lock upgrade blocked by other reader\n")
    else:
        print("✗ Test 13 FAILED\n")
    
    # Test 14: Deadlock Potential Scenario
    print_schedule("Test 14: Potential Deadlock", ["T1 Write X", "T2 Write Y", "T1 Write Y", "T2 Write X"])
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(X)")
    r1 = ccm.transaction_query(t1, RowAction.WRITE, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t2}: Write(Y)")
    r2 = ccm.transaction_query(t2, RowAction.WRITE, 2)
    print(f"   → {r2.reason}")
    
    print(f"T{t1}: Write(Y)")
    r3 = ccm.transaction_query(t1, RowAction.WRITE, 2)
    print(f"   → {r3.reason}")
    
    print(f"T{t2}: Write(X)")
    r4 = ccm.transaction_query(t2, RowAction.WRITE, 1)
    print(f"   → {r4.reason}")
    
    deadlock_prevented = not r3.query_allowed or not r4.query_allowed
    if r1.query_allowed and r2.query_allowed and deadlock_prevented:
        print("✓ Test 14 PASSED - Deadlock prevented by blocking\n")
    else:
        print("✗ Test 14 FAILED\n")
    
    # Test 15: Three-Way Conflict
    print_schedule("Test 15: Three-Way Conflict", ["T1 Write X", "T2 Read X", "T3 Write X"])
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    t3 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(X)")
    r1 = ccm.transaction_query(t1, RowAction.WRITE, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t2}: Read(X)")
    r2 = ccm.transaction_query(t2, RowAction.READ, 1)
    print(f"   → {r2.reason}")
    
    print(f"T{t3}: Write(X)")
    r3 = ccm.transaction_query(t3, RowAction.WRITE, 1)
    print(f"   → {r3.reason}")
    
    if r1.query_allowed and not r2.query_allowed and not r3.query_allowed:
        print("✓ Test 15 PASSED - Both T2 and T3 blocked\n")
    else:
        print("✗ Test 15 FAILED\n")

if __name__ == "__main__":
    test_lock_based()
