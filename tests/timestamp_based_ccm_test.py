import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.timestamp_based_concurrency_control_manager import TimestampBasedConcurrencyControlManager
from src.row_action import TableAction

def print_schedule(name, operations):
    """Print schedule in readable format"""
    print(f"\n{'='*60}")
    print(f"Schedule: {name}")
    print(f"{'='*60}")
    ops_str = " -> ".join(operations)
    print(f"Operations: {ops_str}\n")

def test_timestamp_based():
    """Comprehensive Timestamp-Based CCM Tests"""
    print("\n" + "="*60)
    print("TIMESTAMP-BASED CONCURRENCY CONTROL - COMPREHENSIVE TESTS")
    print("="*60)

    passed_test, total_tests = 0, 0
    
    # Test 1: Serial Schedule (should work)
    print_schedule("Test 1: Serial Schedule", ["T1 Read X", "T1 Write X", "T1 Commit", "T2 Read X", "T2 Commit"])
    ccm = TimestampBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X) [TS={t1}]")
    r1 = ccm.transaction_query(t1, TableAction.READ, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Write(X) [TS={t1}]")
    r2 = ccm.transaction_query(t1, TableAction.WRITE, 1)
    print(f"   → {r2.reason}")
    
    print(f"T{t1}: Commit")
    c1 = ccm.transaction_commit(t1)
    print(f"   → {c1.reason}")
    ccm.transaction_commit_flushed(t1)
    
    print(f"T{t2}: Read(X) [TS={t2}]")
    r3 = ccm.transaction_query(t2, TableAction.READ, 1)
    print(f"   → {r3.reason}")
    
    print(f"T{t2}: Commit")
    c2 = ccm.transaction_commit(t2)
    print(f"   → {c2.reason}")
    ccm.transaction_commit_flushed(t2)
    
    if r1.can_proceed and r2.can_proceed and r3.can_proceed and c1.can_proceed and c2.can_proceed:
        print("✓ Test 1 PASSED\n")
        passed_test += 1
    else:
        print("✗ Test 1 FAILED\n")
    total_tests += 1
    
    # Test 2: Concurrent Reads (all should succeed)
    print_schedule("Test 2: Concurrent Reads", ["T1 Read X", "T2 Read X", "T3 Read X"])
    ccm = TimestampBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    t3 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X) [TS={t1}]")
    r1 = ccm.transaction_query(t1, TableAction.READ, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t2}: Read(X) [TS={t2}]")
    r2 = ccm.transaction_query(t2, TableAction.READ, 1)
    print(f"   → {r2.reason}")
    
    print(f"T{t3}: Read(X) [TS={t3}]")
    r3 = ccm.transaction_query(t3, TableAction.READ, 1)
    print(f"   → {r3.reason}")
    
    if r1.can_proceed and r2.can_proceed and r3.can_proceed:
        print("✓ Test 2 PASSED - All concurrent reads allowed\n")
        passed_test += 1
    else:
        print("✗ Test 2 FAILED\n")
    total_tests += 1
    
    # Test 3: Write-Read Conflict (Older reads newer write - should fail)
    print_schedule("Test 3: Write-Read Conflict", ["T2 Write X", "T1 Read X"])
    ccm = TimestampBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()  # T2 has higher timestamp
    
    print(f"T{t2}: Write(X) [TS={t2}]")
    r1 = ccm.transaction_query(t2, TableAction.WRITE, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Read(X) [TS={t1}]")
    r2 = ccm.transaction_query(t1, TableAction.READ, 1)
    print(f"   → {r2.reason}")
    
    if r1.can_proceed and not r2.can_proceed:
        print("✓ Test 3 PASSED - Older transaction cannot read newer write\n")
        passed_test += 1
    else:
        print("✗ Test 3 FAILED\n")
    total_tests += 1
    
    # Test 4: Read-Write Conflict (Older writes after newer reads - should fail)
    print_schedule("Test 4: Read-Write Conflict", ["T2 Read X", "T1 Write X"])
    ccm = TimestampBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()  # T2 has higher timestamp
    
    print(f"T{t2}: Read(X) [TS={t2}]")
    r1 = ccm.transaction_query(t2, TableAction.READ, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Write(X) [TS={t1}]")
    r2 = ccm.transaction_query(t1, TableAction.WRITE, 1)
    print(f"   → {r2.reason}")
    
    if r1.can_proceed and not r2.can_proceed:
        print("✓ Test 4 PASSED - Older transaction cannot write after newer read\n")
        passed_test += 1
    else:
        print("✗ Test 4 FAILED\n")
    total_tests += 1
    
    # Test 5: Thomas Write Rule (Older write after newer write - should be ignored)
    print_schedule("Test 5: Thomas Write Rule", ["T2 Write X", "T1 Write X"])
    ccm = TimestampBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()  # T2 has higher timestamp
    
    print(f"T{t2}: Write(X) [TS={t2}]")
    r1 = ccm.transaction_query(t2, TableAction.WRITE, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Write(X) [TS={t1}]")
    r2 = ccm.transaction_query(t1, TableAction.WRITE, 1)
    print(f"   → {r2.reason}")
    
    if r1.can_proceed and r2.can_proceed and "Thomas Write Rule" in r2.reason:
        print("✓ Test 5 PASSED - Thomas Write Rule applied\n")
        passed_test += 1
    else:
        print("✗ Test 5 FAILED\n")
    total_tests += 1
    
    # Test 6: Proper Ordering (Newer transaction operations)
    print_schedule("Test 6: Proper Ordering", ["T1 Write X", "T2 Read X", "T2 Write X"])
    ccm = TimestampBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()  # T2 has higher timestamp
    
    print(f"T{t1}: Write(X) [TS={t1}]")
    r1 = ccm.transaction_query(t1, TableAction.WRITE, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t2}: Read(X) [TS={t2}]")
    r2 = ccm.transaction_query(t2, TableAction.READ, 1)
    print(f"   → {r2.reason}")
    
    print(f"T{t2}: Write(X) [TS={t2}]")
    r3 = ccm.transaction_query(t2, TableAction.WRITE, 1)
    print(f"   → {r3.reason}")
    
    if r1.can_proceed and r2.can_proceed and r3.can_proceed:
        print("✓ Test 6 PASSED - Proper timestamp ordering maintained\n")
        passed_test += 1
    else:
        print("✗ Test 6 FAILED\n")
    total_tests += 1
    
    # Test 7: Different Objects (No conflict)
    print_schedule("Test 7: Different Objects", ["T1 Write X", "T2 Write Y"])
    ccm = TimestampBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(X) [TS={t1}]")
    r1 = ccm.transaction_query(t1, TableAction.WRITE, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t2}: Write(Y) [TS={t2}]")
    r2 = ccm.transaction_query(t2, TableAction.WRITE, 2)
    print(f"   → {r2.reason}")
    
    if r1.can_proceed and r2.can_proceed:
        print("✓ Test 7 PASSED - Different objects don't conflict\n")
        passed_test += 1
    else:
        print("✗ Test 7 FAILED\n")
    total_tests += 1
    
    # Test 8: Multiple Reads Update RTS
    print_schedule("Test 8: Multiple Reads Update RTS", ["T1 Read X", "T2 Read X", "T3 Read X"])
    ccm = TimestampBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    t3 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X) [TS={t1}]")
    r1 = ccm.transaction_query(t1, TableAction.READ, 1)
    print(f"   → {r1.reason}")
    print(f"   RTS(X) = {ccm.table_read_timestamps.get(1, 0)}")
    
    print(f"T{t2}: Read(X) [TS={t2}]")
    r2 = ccm.transaction_query(t2, TableAction.READ, 1)
    print(f"   → {r2.reason}")
    print(f"   RTS(X) = {ccm.table_read_timestamps.get(1, 0)}")
    
    print(f"T{t3}: Read(X) [TS={t3}]")
    r3 = ccm.transaction_query(t3, TableAction.READ, 1)
    print(f"   → {r3.reason}")
    print(f"   RTS(X) = {ccm.table_read_timestamps.get(1, 0)}")
    
    if r1.can_proceed and r2.can_proceed and r3.can_proceed and ccm.table_read_timestamps.get(1, 0) == t3:
        print("✓ Test 8 PASSED - RTS properly updated to maximum\n")
        passed_test += 1
    else:
        print("✗ Test 8 FAILED\n")
    total_tests += 1
    
    # Test 9: Commit Validation Success
    print_schedule("Test 9: Commit Validation Success", ["T1 Read X", "T1 Write Y", "T1 Commit"])
    ccm = TimestampBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X) [TS={t1}]")
    r1 = ccm.transaction_query(t1, TableAction.READ, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Write(Y) [TS={t1}]")
    r2 = ccm.transaction_query(t1, TableAction.WRITE, 2)
    print(f"   → {r2.reason}")
    
    print(f"T{t1}: Commit")
    c1 = ccm.transaction_commit(t1)
    print(f"   → {c1.reason}")
    
    if r1.can_proceed and r2.can_proceed and c1.can_proceed:
        print("✓ Test 9 PASSED - Commit validation successful\n")
        passed_test += 1
    else:
        print("✗ Test 9 FAILED\n")
    total_tests += 1
    
    # Test 10: Commit Validation Failure
    print_schedule("Test 10: Commit Validation Failure", ["T1 Read X", "T2 Write X", "T1 Commit"])
    ccm = TimestampBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X) [TS={t1}]")
    r1 = ccm.transaction_query(t1, TableAction.READ, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t2}: Write(X) [TS={t2}]")
    r2 = ccm.transaction_query(t2, TableAction.WRITE, 1)
    print(f"   → {r2.reason}")
    
    print(f"T{t1}: Commit")
    c1 = ccm.transaction_commit(t1)
    print(f"   → {c1.reason}")
    
    if r1.can_proceed and r2.can_proceed and not c1.can_proceed:
        print("✓ Test 10 PASSED - Commit validation failed (read set invalidated)\n")
        passed_test += 1
    else:
        print("✗ Test 10 FAILED\n")
    total_tests += 1
    
    # Test 11: Same Transaction Multiple Operations
    print_schedule("Test 11: Same Transaction Multiple Ops", ["T1 Read X", "T1 Write X", "T1 Read Y", "T1 Write Y"])
    ccm = TimestampBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X) [TS={t1}]")
    r1 = ccm.transaction_query(t1, TableAction.READ, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Write(X) [TS={t1}]")
    r2 = ccm.transaction_query(t1, TableAction.WRITE, 1)
    print(f"   → {r2.reason}")
    
    print(f"T{t1}: Read(Y) [TS={t1}]")
    r3 = ccm.transaction_query(t1, TableAction.READ, 2)
    print(f"   → {r3.reason}")
    
    print(f"T{t1}: Write(Y) [TS={t1}]")
    r4 = ccm.transaction_query(t1, TableAction.WRITE, 2)
    print(f"   → {r4.reason}")
    
    if r1.can_proceed and r2.can_proceed and r3.can_proceed and r4.can_proceed:
        print("✓ Test 11 PASSED - Same transaction multiple operations\n")
        passed_test += 1
    else:
        print("✗ Test 11 FAILED\n")
    total_tests += 1
    
    # Test 12: Complex Multi-Object Transaction
    print_schedule("Test 12: Multi-Object Transaction", ["T1 Write X", "T1 Read Y", "T2 Read X", "T2 Write Y"])
    ccm = TimestampBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(X) [TS={t1}]")
    r1 = ccm.transaction_query(t1, TableAction.WRITE, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Read(Y) [TS={t1}]")
    r2 = ccm.transaction_query(t1, TableAction.READ, 2)
    print(f"   → {r2.reason}")
    
    print(f"T{t2}: Read(X) [TS={t2}]")
    r3 = ccm.transaction_query(t2, TableAction.READ, 1)
    print(f"   → {r3.reason}")
    
    print(f"T{t2}: Write(Y) [TS={t2}]")
    r4 = ccm.transaction_query(t2, TableAction.WRITE, 2)
    print(f"   → {r4.reason}")
    
    # T2 has higher timestamp, so it should succeed
    if r1.can_proceed and r2.can_proceed and r3.can_proceed and r4.can_proceed:
        print("✓ Test 12 PASSED - Multi-object operations work correctly\n")
        passed_test += 1
    else:
        print("✗ Test 12 FAILED\n")
    total_tests += 1
    
    # Test 13: Cascading Abort Prevention
    print_schedule("Test 13: Dirty Read Prevention", ["T1 Write X", "T2 Read X (should succeed)", "T1 Commit"])
    ccm = TimestampBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(X) [TS={t1}]")
    r1 = ccm.transaction_query(t1, TableAction.WRITE, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t2}: Read(X) [TS={t2}]")
    r2 = ccm.transaction_query(t2, TableAction.READ, 1)
    print(f"   → {r2.reason}")
    
    # In timestamp ordering, T2 can read because it has higher TS
    if r1.can_proceed and r2.can_proceed:
        print("✓ Test 13 PASSED - Timestamp ordering allows read (no dirty read in TO)\n")
        passed_test += 1
    else:
        print("✗ Test 13 FAILED\n")
    total_tests += 1
    
    # Test 14: Write Timestamp Updates
    print_schedule("Test 14: Write Timestamp Updates", ["T1 Write X", "T2 Write X", "T3 Write X"])
    ccm = TimestampBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    t3 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(X) [TS={t1}]")
    r1 = ccm.transaction_query(t1, TableAction.WRITE, 1)
    print(f"   → {r1.reason}")
    print(f"   WTS(X) = {ccm.table_write_timestamps.get(1, 0)}")
    
    print(f"T{t2}: Write(X) [TS={t2}]")
    r2 = ccm.transaction_query(t2, TableAction.WRITE, 1)
    print(f"   → {r2.reason}")
    print(f"   WTS(X) = {ccm.table_write_timestamps.get(1, 0)}")
    
    print(f"T{t3}: Write(X) [TS={t3}]")
    r3 = ccm.transaction_query(t3, TableAction.WRITE, 1)
    print(f"   → {r3.reason}")
    print(f"   WTS(X) = {ccm.table_write_timestamps.get(1, 0)}")
    
    if r1.can_proceed and r2.can_proceed and r3.can_proceed and ccm.table_write_timestamps.get(1, 0) == t3:
        print("✓ Test 14 PASSED - WTS properly updated\n")
        passed_test += 1
    else:
        print("✗ Test 14 FAILED\n")
    total_tests += 1
    
    # Test 15: Read-Write-Read Pattern
    print_schedule("Test 15: Read-Write-Read Pattern", ["T1 Read X", "T2 Write X", "T3 Read X"])
    ccm = TimestampBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    t3 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X) [TS={t1}]")
    r1 = ccm.transaction_query(t1, TableAction.READ, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t2}: Write(X) [TS={t2}]")
    r2 = ccm.transaction_query(t2, TableAction.WRITE, 1)
    print(f"   → {r2.reason}")
    
    print(f"T{t3}: Read(X) [TS={t3}]")
    r3 = ccm.transaction_query(t3, TableAction.READ, 1)
    print(f"   → {r3.reason}")
    
    if r1.can_proceed and r2.can_proceed and r3.can_proceed:
        print("✓ Test 15 PASSED - Read-Write-Read pattern works\n")
        passed_test += 1
    else:
        print("✗ Test 15 FAILED\n")
    total_tests += 1
    
    # Test 16: Empty Read Set Commit
    print_schedule("Test 16: Write-Only Transaction Commit", ["T1 Write X", "T1 Write Y", "T1 Commit"])
    ccm = TimestampBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(X) [TS={t1}]")
    r1 = ccm.transaction_query(t1, TableAction.WRITE, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t1}: Write(Y) [TS={t1}]")
    r2 = ccm.transaction_query(t1, TableAction.WRITE, 2)
    print(f"   → {r2.reason}")
    
    print(f"T{t1}: Commit")
    c1 = ccm.transaction_commit(t1)
    print(f"   → {c1.reason}")
    
    if r1.can_proceed and r2.can_proceed and c1.can_proceed:
        print("✓ Test 16 PASSED - Write-only transaction commits successfully\n")
        passed_test += 1
    else:
        print("✗ Test 16 FAILED\n")
    total_tests += 1
    
    # Test 17: Multiple Transactions on Multiple Objects
    print_schedule("Test 17: Multiple Transactions, Multiple Objects", 
                  ["T1 Write X", "T2 Write Y", "T3 Write Z", "T1 Read Y", "T2 Read Z", "T3 Read X"])
    ccm = TimestampBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    t3 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(X) [TS={t1}]")
    r1 = ccm.transaction_query(t1, TableAction.WRITE, 1)
    print(f"   → {r1.reason}")
    
    print(f"T{t2}: Write(Y) [TS={t2}]")
    r2 = ccm.transaction_query(t2, TableAction.WRITE, 2)
    print(f"   → {r2.reason}")
    
    print(f"T{t3}: Write(Z) [TS={t3}]")
    r3 = ccm.transaction_query(t3, TableAction.WRITE, 3)
    print(f"   → {r3.reason}")
    
    print(f"T{t1}: Read(Y) [TS={t1}]")
    r4 = ccm.transaction_query(t1, TableAction.READ, 2)
    print(f"   → {r4.reason}")
    
    print(f"T{t2}: Read(Z) [TS={t2}]")
    r5 = ccm.transaction_query(t2, TableAction.READ, 3)
    print(f"   → {r5.reason}")
    
    print(f"T{t3}: Read(X) [TS={t3}]")
    r6 = ccm.transaction_query(t3, TableAction.READ, 1)
    print(f"   → {r6.reason}")
    
    # T1 tries to read Y (written by T2 with higher TS) - should fail
    # T2 tries to read Z (written by T3 with higher TS) - should fail
    # T3 tries to read X (written by T1 with lower TS) - should succeed
    conflicts_detected = not r4.can_proceed or not r5.can_proceed
    if r1.can_proceed and r2.can_proceed and r3.can_proceed and r6.can_proceed and conflicts_detected:
        print("✓ Test 17 PASSED - Complex multi-object conflicts detected\n")
        passed_test += 1
    else:
        print("✗ Test 17 FAILED\n")
    total_tests += 1
    
    print("\n" + "="*60)
    print("TIMESTAMP-BASED CCM TESTS COMPLETED")
    print(f"Passed {passed_test} out of {total_tests} tests.")
    print("="*60)

if __name__ == "__main__":
    test_timestamp_based()
