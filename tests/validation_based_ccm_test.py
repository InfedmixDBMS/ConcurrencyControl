import sys
import os
import time
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.validation_based_concurrency_control_manager import ValidationBasedConcurrencyControlManager
from src.row_action import TableAction

def print_schedule(name, operations):
    """Print schedule in readable format"""
    print(f"\n{'='*60}")
    print(f"Schedule: {name}")
    print(f"{'='*60}")
    ops_str = " -> ".join(operations)
    print(f"Operations: {ops_str}\n")

def test_validation_based():
    """Comprehensive Validation-Based CCM Tests"""
    print("\n" + "="*60)
    print("VALIDATION-BASED CONCURRENCY CONTROL - COMPREHENSIVE TESTS")
    print("="*60)

    passed_test, total_tests = 0, 0
    
    # Test 1: Serial Schedule (should work)
    print_schedule("Test 1: Serial Schedule", ["T1 Read X", "T1 Write Y", "T1 Commit", "T2 Read X", "T2 Write Z", "T2 Commit"])
    ccm = ValidationBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X)")
    r1 = ccm.transaction_query(t1, TableAction.READ, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Write(Y)")
    r2 = ccm.transaction_query(t1, TableAction.WRITE, 2)
    print(f"   → {r2.reason}")
    
    print(f"T{t1}: Commit")
    c1 = ccm.transaction_commit(t1)
    print(f"   → {c1.reason}")
    ccm.transaction_commit_flushed(t1)
    
    t2 = ccm.transaction_begin()
    
    print(f"T{t2}: Read(X)")
    r3 = ccm.transaction_query(t2, TableAction.READ, 1)
    print(f"   → {r3.reason}")
    
    print(f"T{t2}: Write(Z)")
    r4 = ccm.transaction_query(t2, TableAction.WRITE, 3)
    print(f"   → {r4.reason}")
    
    print(f"T{t2}: Commit")
    c2 = ccm.transaction_commit(t2)
    print(f"   → {c2.reason}")
    ccm.transaction_commit_flushed(t2)
    
    if r1.query_allowed and r2.query_allowed and r3.query_allowed and r4.query_allowed and c1.query_allowed and c2.query_allowed:
        print("✓ Test 1 PASSED\n")
        passed_test += 1
    else:
        print("✗ Test 1 FAILED\n")
    total_tests += 1
    
    # Test 2: Read Phase - All Reads Allowed
    print_schedule("Test 2: Read Phase", ["T1 Read X", "T1 Read Y", "T1 Read Z"])
    ccm = ValidationBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X)")
    r1 = ccm.transaction_query(t1, TableAction.READ, 1)
    print(f"   → {r1.reason}")
    print(f"   Read set: {ccm.transactions[t1]['read_set']}")
    
    print(f"T{t1}: Read(Y)")
    r2 = ccm.transaction_query(t1, TableAction.READ, 2)
    print(f"   → {r2.reason}")
    print(f"   Read set: {ccm.transactions[t1]['read_set']}")
    
    print(f"T{t1}: Read(Z)")
    r3 = ccm.transaction_query(t1, TableAction.READ, 3)
    print(f"   → {r3.reason}")
    print(f"   Read set: {ccm.transactions[t1]['read_set']}")
    
    if r1.query_allowed and r2.query_allowed and r3.query_allowed and ccm.transactions[t1]['read_set'] == {1, 2, 3}:
        print("✓ Test 2 PASSED - Read set tracked correctly\n")
        passed_test += 1
    else:
        print("✗ Test 2 FAILED\n")
    total_tests += 1
    
    # Test 3: Write Phase - All Writes Allowed
    print_schedule("Test 3: Write Phase", ["T1 Write X", "T1 Write Y", "T1 Write Z"])
    ccm = ValidationBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(X)")
    r1 = ccm.transaction_query(t1, TableAction.WRITE, 1)
    print(f"   → {r1.reason}")
    print(f"   Write set: {ccm.transactions[t1]['write_set']}")
    
    print(f"T{t1}: Write(Y)")
    r2 = ccm.transaction_query(t1, TableAction.WRITE, 2)
    print(f"   → {r2.reason}")
    print(f"   Write set: {ccm.transactions[t1]['write_set']}")
    
    print(f"T{t1}: Write(Z)")
    r3 = ccm.transaction_query(t1, TableAction.WRITE, 3)
    print(f"   → {r3.reason}")
    print(f"   Write set: {ccm.transactions[t1]['write_set']}")
    
    if r1.query_allowed and r2.query_allowed and r3.query_allowed and ccm.transactions[t1]['write_set'] == {1, 2, 3}:
        print("✓ Test 3 PASSED - Write set tracked correctly\n")
        passed_test += 1
    else:
        print("✗ Test 3 FAILED\n")
    total_tests += 1
    
    # Test 4: Read-Write Conflict Detection
    print_schedule("Test 4: Read-Write Conflict", ["T1 Read X", "T2 Write X", "T2 Commit", "T1 Commit"])
    ccm = ValidationBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    time.sleep(0.01)  # Ensure different timestamps
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X)")
    r1 = ccm.transaction_query(t1, TableAction.READ, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t2}: Write(X)")
    r2 = ccm.transaction_query(t2, TableAction.WRITE, 1)
    print(f"   → {r2.reason}")
    
    print(f"T{t2}: Commit")
    c2 = ccm.transaction_commit(t2)
    print(f"   → {c2.reason}")
    ccm.transaction_commit_flushed(t2)
    
    time.sleep(0.01)  # Ensure finish timestamp is set
    
    print(f"T{t1}: Commit")
    c1 = ccm.transaction_commit(t1)
    print(f"   → {c1.reason}")
    
    if r1.query_allowed and r2.query_allowed and c2.query_allowed and not c1.query_allowed:
        print("✓ Test 4 PASSED - Read-Write conflict detected\n")
        passed_test += 1
    else:
        print("✗ Test 4 FAILED\n")
    total_tests += 1
    
    # Test 5: Write-Write Conflict Detection
    print_schedule("Test 5: Write-Write Conflict", ["T1 Write X", "T2 Write X", "T2 Commit", "T1 Commit"])
    ccm = ValidationBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    time.sleep(0.01)
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(X)")
    r1 = ccm.transaction_query(t1, TableAction.WRITE, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t2}: Write(X)")
    r2 = ccm.transaction_query(t2, TableAction.WRITE, 1)
    print(f"   → {r2.reason}")
    
    print(f"T{t2}: Commit")
    c2 = ccm.transaction_commit(t2)
    print(f"   → {c2.reason}")
    ccm.transaction_commit_flushed(t2)
    
    time.sleep(0.01)
    
    print(f"T{t1}: Commit")
    c1 = ccm.transaction_commit(t1)
    print(f"   → {c1.reason}")
    
    if r1.query_allowed and r2.query_allowed and c2.query_allowed and not c1.query_allowed:
        print("✓ Test 5 PASSED - Write-Write conflict detected\n")
        passed_test += 1
    else:
        print("✗ Test 5 FAILED\n")
    total_tests += 1
    
    # Test 6: No Conflict - Different Objects
    print_schedule("Test 6: No Conflict - Different Objects", ["T1 Write X", "T2 Write Y", "T2 Commit", "T1 Commit"])
    ccm = ValidationBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    time.sleep(0.01)
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(X)")
    r1 = ccm.transaction_query(t1, TableAction.WRITE, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t2}: Write(Y)")
    r2 = ccm.transaction_query(t2, TableAction.WRITE, 2)
    print(f"   → {r2.reason}")
    
    print(f"T{t2}: Commit")
    c2 = ccm.transaction_commit(t2)
    print(f"   → {c2.reason}")
    ccm.transaction_commit_flushed(t2)
    
    time.sleep(0.01)
    
    print(f"T{t1}: Commit")
    c1 = ccm.transaction_commit(t1)
    print(f"   → {c1.reason}")
    
    if r1.query_allowed and r2.query_allowed and c2.query_allowed and c1.query_allowed:
        print("✓ Test 6 PASSED - No conflict on different objects\n")
        passed_test += 1
    else:
        print("✗ Test 6 FAILED\n")
    total_tests += 1
    
    # Test 7: Validation Window - T2 finishes before T1 starts
    print_schedule("Test 7: No Validation - T2 finishes before T1 starts", ["T2 Write X", "T2 Commit", "T1 Read X", "T1 Commit"])
    ccm = ValidationBasedConcurrencyControlManager()
    t2 = ccm.transaction_begin()
    
    print(f"T{t2}: Write(X)")
    r1 = ccm.transaction_query(t2, TableAction.WRITE, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t2}: Commit")
    c2 = ccm.transaction_commit(t2)
    print(f"   → {c2.reason}")
    ccm.transaction_commit_flushed(t2)
    
    time.sleep(0.01)  # Ensure T1 starts after T2 finishes
    
    t1 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X)")
    r2 = ccm.transaction_query(t1, TableAction.READ, 1)
    print(f"   → {r2.reason}")
    
    print(f"T{t1}: Commit")
    c1 = ccm.transaction_commit(t1)
    print(f"   → {c1.reason}")
    
    if r1.query_allowed and r2.query_allowed and c2.query_allowed and c1.query_allowed:
        print("✓ Test 7 PASSED - No validation needed (T2 finished before T1 started)\n")
        passed_test += 1
    else:
        print("✗ Test 7 FAILED\n")
    total_tests += 1
    
    # Test 8: Validation Window - T2 starts after T1 validates
    print_schedule("Test 8: No Validation - T2 starts after T1 validates", ["T1 Read X", "T1 Commit", "T2 Write X", "T2 Commit"])
    ccm = ValidationBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X)")
    r1 = ccm.transaction_query(t1, TableAction.READ, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Commit")
    c1 = ccm.transaction_commit(t1)
    print(f"   → {c1.reason}")
    ccm.transaction_commit_flushed(t1)
    
    time.sleep(0.01)  # Ensure T2 starts after T1 validates
    
    t2 = ccm.transaction_begin()
    
    print(f"T{t2}: Write(X)")
    r2 = ccm.transaction_query(t2, TableAction.WRITE, 1)
    print(f"   → {r2.reason}")
    
    print(f"T{t2}: Commit")
    c2 = ccm.transaction_commit(t2)
    print(f"   → {c2.reason}")
    
    if r1.query_allowed and r2.query_allowed and c1.query_allowed and c2.query_allowed:
        print("✓ Test 8 PASSED - No validation needed (T2 started after T1 validated)\n")
        passed_test += 1
    else:
        print("✗ Test 8 FAILED\n")
    total_tests += 1
    
    # Test 9: Read-Only Transaction (always succeeds)
    print_schedule("Test 9: Read-Only Transaction", ["T1 Read X", "T1 Read Y", "T1 Commit"])
    ccm = ValidationBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X)")
    r1 = ccm.transaction_query(t1, TableAction.READ, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Read(Y)")
    r2 = ccm.transaction_query(t1, TableAction.READ, 2)
    print(f"   → {r2.reason}")
    
    print(f"T{t1}: Commit")
    c1 = ccm.transaction_commit(t1)
    print(f"   → {c1.reason}")
    
    if r1.query_allowed and r2.query_allowed and c1.query_allowed:
        print("✓ Test 9 PASSED - Read-only transaction commits successfully\n")
        passed_test += 1
    else:
        print("✗ Test 9 FAILED\n")
    total_tests += 1
    
    # Test 10: Write-Only Transaction
    print_schedule("Test 10: Write-Only Transaction", ["T1 Write X", "T1 Write Y", "T1 Commit"])
    ccm = ValidationBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(X)")
    r1 = ccm.transaction_query(t1, TableAction.WRITE, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Write(Y)")
    r2 = ccm.transaction_query(t1, TableAction.WRITE, 2)
    print(f"   → {r2.reason}")
    
    print(f"T{t1}: Commit")
    c1 = ccm.transaction_commit(t1)
    print(f"   → {c1.reason}")
    
    if r1.query_allowed and r2.query_allowed and c1.query_allowed:
        print("✓ Test 10 PASSED - Write-only transaction commits successfully\n")
        passed_test += 1
    else:
        print("✗ Test 10 FAILED\n")
    total_tests += 1
    
    # Test 11: Multiple Overlapping Transactions
    print_schedule("Test 11: Multiple Overlapping Transactions", 
                  ["T1 Read X", "T2 Read X", "T3 Read X", "T1 Write Y", "T2 Write Y", "T1 Commit", "T2 Commit"])
    ccm = ValidationBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    time.sleep(0.01)
    t2 = ccm.transaction_begin()
    time.sleep(0.01)
    t3 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X)")
    r1 = ccm.transaction_query(t1, TableAction.READ, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t2}: Read(X)")
    r2 = ccm.transaction_query(t2, TableAction.READ, 1)
    print(f"   → {r2.reason}")
    
    print(f"T{t3}: Read(X)")
    r3 = ccm.transaction_query(t3, TableAction.READ, 1)
    print(f"   → {r3.reason}")
    
    print(f"T{t1}: Write(Y)")
    r4 = ccm.transaction_query(t1, TableAction.WRITE, 2)
    print(f"   → {r4.reason}")
    
    print(f"T{t2}: Write(Y)")
    r5 = ccm.transaction_query(t2, TableAction.WRITE, 2)
    print(f"   → {r5.reason}")
    
    print(f"T{t1}: Commit")
    c1 = ccm.transaction_commit(t1)
    print(f"   → {c1.reason}")
    ccm.transaction_commit_flushed(t1)
    
    time.sleep(0.01)
    
    print(f"T{t2}: Commit")
    c2 = ccm.transaction_commit(t2)
    print(f"   → {c2.reason}")
    
    if r1.query_allowed and r2.query_allowed and r3.query_allowed and r4.query_allowed and r5.query_allowed and c1.query_allowed and not c2.query_allowed:
        print("✓ Test 11 PASSED - T2 fails due to write-write conflict with T1\n")
        passed_test += 1
    else:
        print("✗ Test 11 FAILED\n")
    total_tests += 1
    
    # Test 12: Complex Multi-Object Transaction
    print_schedule("Test 12: Complex Multi-Object Transaction", 
                  ["T1 Read X", "T1 Read Y", "T2 Write X", "T2 Write Z", "T2 Commit", "T1 Write Y", "T1 Commit"])
    ccm = ValidationBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    time.sleep(0.01)
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X)")
    r1 = ccm.transaction_query(t1, TableAction.READ, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Read(Y)")
    r2 = ccm.transaction_query(t1, TableAction.READ, 2)
    print(f"   → {r2.reason}")
    
    print(f"T{t2}: Write(X)")
    r3 = ccm.transaction_query(t2, TableAction.WRITE, 1)
    print(f"   → {r3.reason}")
    
    print(f"T{t2}: Write(Z)")
    r4 = ccm.transaction_query(t2, TableAction.WRITE, 3)
    print(f"   → {r4.reason}")
    
    print(f"T{t2}: Commit")
    c2 = ccm.transaction_commit(t2)
    print(f"   → {c2.reason}")
    ccm.transaction_commit_flushed(t2)
    
    time.sleep(0.01)
    
    print(f"T{t1}: Write(Y)")
    r5 = ccm.transaction_query(t1, TableAction.WRITE, 2)
    print(f"   → {r5.reason}")
    
    print(f"T{t1}: Commit")
    c1 = ccm.transaction_commit(t1)
    print(f"   → {c1.reason}")
    
    # T1 should fail because T2 wrote X which T1 read
    if r1.query_allowed and r2.query_allowed and r3.query_allowed and r4.query_allowed and r5.query_allowed and c2.query_allowed and not c1.query_allowed:
        print("✓ Test 12 PASSED - Complex conflict detected\n")
        passed_test += 1
    else:
        print("✗ Test 12 FAILED\n")
    total_tests += 1
    
    # Test 13: Partial Overlap - Read Set Intersection
    print_schedule("Test 13: Partial Overlap", 
                  ["T1 Read X", "T1 Read Y", "T2 Write Y", "T2 Write Z", "T2 Commit", "T1 Commit"])
    ccm = ValidationBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    time.sleep(0.01)
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X)")
    r1 = ccm.transaction_query(t1, TableAction.READ, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Read(Y)")
    r2 = ccm.transaction_query(t1, TableAction.READ, 2)
    print(f"   → {r2.reason}")
    
    print(f"T{t2}: Write(Y)")
    r3 = ccm.transaction_query(t2, TableAction.WRITE, 2)
    print(f"   → {r3.reason}")
    
    print(f"T{t2}: Write(Z)")
    r4 = ccm.transaction_query(t2, TableAction.WRITE, 3)
    print(f"   → {r4.reason}")
    
    print(f"T{t2}: Commit")
    c2 = ccm.transaction_commit(t2)
    print(f"   → {c2.reason}")
    ccm.transaction_commit_flushed(t2)
    
    time.sleep(0.01)
    
    print(f"T{t1}: Commit")
    c1 = ccm.transaction_commit(t1)
    print(f"   → {c1.reason}")
    
    # T1 read Y, T2 wrote Y, so conflict
    if r1.query_allowed and r2.query_allowed and r3.query_allowed and r4.query_allowed and c2.query_allowed and not c1.query_allowed:
        print("✓ Test 13 PASSED - Partial overlap conflict detected\n")
        passed_test += 1
    else:
        print("✗ Test 13 FAILED\n")
    total_tests += 1
    
    # Test 14: Three Transactions - Sequential Success
    print_schedule("Test 14: Three Sequential Transactions", 
                  ["T1 Write X", "T1 Commit", "T2 Read X", "T2 Commit", "T3 Write X", "T3 Commit"])
    ccm = ValidationBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(X)")
    r1 = ccm.transaction_query(t1, TableAction.WRITE, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Commit")
    c1 = ccm.transaction_commit(t1)
    print(f"   → {c1.reason}")
    ccm.transaction_commit_flushed(t1)
    
    time.sleep(0.01)
    
    t2 = ccm.transaction_begin()
    
    print(f"T{t2}: Read(X)")
    r2 = ccm.transaction_query(t2, TableAction.READ, 1)
    print(f"   → {r2.reason}")
    
    print(f"T{t2}: Commit")
    c2 = ccm.transaction_commit(t2)
    print(f"   → {c2.reason}")
    ccm.transaction_commit_flushed(t2)
    
    time.sleep(0.01)
    
    t3 = ccm.transaction_begin()
    
    print(f"T{t3}: Write(X)")
    r3 = ccm.transaction_query(t3, TableAction.WRITE, 1)
    print(f"   → {r3.reason}")
    
    print(f"T{t3}: Commit")
    c3 = ccm.transaction_commit(t3)
    print(f"   → {c3.reason}")
    
    if all([r1.query_allowed, r2.query_allowed, r3.query_allowed, c1.query_allowed, c2.query_allowed, c3.query_allowed]):
        print("✓ Test 14 PASSED - All sequential transactions succeed\n")
        passed_test += 1
    else:
        print("✗ Test 14 FAILED\n")
    total_tests += 1
    
    # Test 15: Mixed Read-Write Sets
    print_schedule("Test 15: Mixed Read-Write Sets", 
                  ["T1 Read X", "T1 Write Y", "T2 Read Y", "T2 Write X", "T1 Commit", "T2 Commit"])
    ccm = ValidationBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    time.sleep(0.01)
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X)")
    r1 = ccm.transaction_query(t1, TableAction.READ, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Write(Y)")
    r2 = ccm.transaction_query(t1, TableAction.WRITE, 2)
    print(f"   → {r2.reason}")
    
    print(f"T{t2}: Read(Y)")
    r3 = ccm.transaction_query(t2, TableAction.READ, 2)
    print(f"   → {r3.reason}")
    
    print(f"T{t2}: Write(X)")
    r4 = ccm.transaction_query(t2, TableAction.WRITE, 1)
    print(f"   → {r4.reason}")
    
    print(f"T{t1}: Commit")
    c1 = ccm.transaction_commit(t1)
    print(f"   → {c1.reason}")
    ccm.transaction_commit_flushed(t1)
    
    time.sleep(0.01)
    
    print(f"T{t2}: Commit")
    c2 = ccm.transaction_commit(t2)
    print(f"   → {c2.reason}")
    
    # T2 should fail: T1 wrote Y (which T2 read) and T2 wrote X (which T1 read)
    if all([r1.query_allowed, r2.query_allowed, r3.query_allowed, r4.query_allowed, c1.query_allowed]) and not c2.query_allowed:
        print("✓ Test 15 PASSED - Bidirectional conflict detected\n")
        passed_test += 1
    else:
        print("✗ Test 15 FAILED\n")
    total_tests += 1
    
    print("\n" + "="*60)
    print("VALIDATION-BASED CCM TESTS COMPLETED")
    print(f"Passed {passed_test} out of {total_tests} tests.")
    print("="*60)

if __name__ == "__main__":
    test_validation_based()