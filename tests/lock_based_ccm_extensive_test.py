import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.lock_based_concurrency_control_manager import LockBasedConcurrencyControlManager
from src.row_action import TableAction
from src.concurrency_response import LockStatus

def print_test_header(test_num, name):
    print(f"\n{'='*70}")
    print(f"Test {test_num}: {name}")
    print(f"{'='*70}")

def test_extensive_lock_based():
    print("\n" + "="*70)
    print("EXTENSIVE LOCK-BASED CCM TESTS - 2PL + WAIT-DIE")
    print("="*70)
    
    passed = 0
    failed = 0
    
    #test 1: cascading aborts scenario
    print_test_header(1, "Cascading Abort Prevention (Strict 2PL)")
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    t3 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(A)")
    ccm.transaction_query(t1, TableAction.WRITE, 'A')
    
    print(f"T{t2}: Read(A) - blocked/aborted by T{t1}")
    r2 = ccm.transaction_query(t2, TableAction.READ, 'A')
    
    print(f"T{t1}: Abort")
    ccm.transaction_rollback(t1)
    ccm.transaction_abort(t1)
    
    print(f"T{t3}: Read(A)")
    r3 = ccm.transaction_query(t3, TableAction.READ, 'A')
    
    if not r2.query_allowed and r3.query_allowed:
        print("✓ PASSED - T2 blocked, T3 can read (no cascading abort)")
        passed += 1
    else:
        print("✗ FAILED")
        failed += 1
    
    #test 2: lock escalation with multiple objects
    print_test_header(2, "Lock Escalation Across Multiple Objects")
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(A), Read(B), Read(C)")
    r1 = ccm.transaction_query(t1, TableAction.READ, 'A')
    r2 = ccm.transaction_query(t1, TableAction.READ, 'B')
    r3 = ccm.transaction_query(t1, TableAction.READ, 'C')
    
    print(f"T{t1}: Write(A), Write(B), Write(C) - upgrade all")
    r4 = ccm.transaction_query(t1, TableAction.WRITE, 'A')
    r5 = ccm.transaction_query(t1, TableAction.WRITE, 'B')
    r6 = ccm.transaction_query(t1, TableAction.WRITE, 'C')
    
    if all([r1.query_allowed, r2.query_allowed, r3.query_allowed, 
            r4.query_allowed, r5.query_allowed, r6.query_allowed]):
        print("✓ PASSED - All locks upgraded successfully")
        passed += 1
    else:
        print("✗ FAILED")
        failed += 1
    
    #test 3: interleaved transactions with different objects
    print_test_header(3, "Interleaved Transactions - No Conflicts")
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    t3 = ccm.transaction_begin()
    
    print(f"Interleaving: T{t1}(A), T{t2}(B), T{t3}(C), T{t1}(D), T{t2}(E), T{t3}(F)")
    r1 = ccm.transaction_query(t1, TableAction.WRITE, 'A')
    r2 = ccm.transaction_query(t2, TableAction.WRITE, 'B')
    r3 = ccm.transaction_query(t3, TableAction.WRITE, 'C')
    r4 = ccm.transaction_query(t1, TableAction.WRITE, 'D')
    r5 = ccm.transaction_query(t2, TableAction.WRITE, 'E')
    r6 = ccm.transaction_query(t3, TableAction.WRITE, 'F')
    
    if all([r1.query_allowed, r2.query_allowed, r3.query_allowed,
            r4.query_allowed, r5.query_allowed, r6.query_allowed]):
        print("✓ PASSED - All non-conflicting operations succeeded")
        passed += 1
    else:
        print("✗ FAILED")
        failed += 1
    
    #test 4: high contention scenario
    print_test_header(4, "High Contention - 5 Transactions on Same Object")
    ccm = LockBasedConcurrencyControlManager()
    transactions = [ccm.transaction_begin() for _ in range(5)]
    
    results = []
    for i, tid in enumerate(transactions):
        print(f"T{tid}: Write(X)")
        r = ccm.transaction_query(tid, TableAction.WRITE, 'X')
        results.append(r.query_allowed)
        if not r.query_allowed:
            print(f"   → {r.reason}")
    
    granted = sum(results)
    if granted == 1:
        print(f"✓ PASSED - Only 1 transaction granted lock, {len(transactions)-1} blocked/aborted")
        passed += 1
    else:
        print(f"✗ FAILED - {granted} transactions got lock (should be 1)")
        failed += 1
    
    #test 5: deadlock scenario with 3 transactions
    print_test_header(5, "Deadlock Prevention - 3-Way Circular Wait")
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    t3 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(A), T{t2}: Write(B), T{t3}: Write(C)")
    ccm.transaction_query(t1, TableAction.WRITE, 'A')
    ccm.transaction_query(t2, TableAction.WRITE, 'B')
    ccm.transaction_query(t3, TableAction.WRITE, 'C')
    
    print(f"T{t1}: Write(B), T{t2}: Write(C), T{t3}: Write(A)")
    r1 = ccm.transaction_query(t1, TableAction.WRITE, 'B')
    r2 = ccm.transaction_query(t2, TableAction.WRITE, 'C')
    r3 = ccm.transaction_query(t3, TableAction.WRITE, 'A')
    
    blocked_or_aborted = sum([not r.query_allowed for r in [r1, r2, r3]])
    if blocked_or_aborted >= 2:
        print("✓ PASSED - At least 2 transactions blocked/aborted (deadlock prevented)")
        passed += 1
    else:
        print("✗ FAILED - Deadlock not prevented")
        failed += 1
    
    #test 6: repeated lock upgrade attempts
    print_test_header(6, "Repeated Lock Upgrade Attempts")
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    print(f"T{t1}: Read(X)")
    ccm.transaction_query(t1, TableAction.READ, 'X')
    
    print(f"T{t1}: Write(X) x5 attempts")
    results = []
    for i in range(5):
        r = ccm.transaction_query(t1, TableAction.WRITE, 'X')
        results.append(r.query_allowed)
    
    if all(results):
        print("✓ PASSED - All upgrade attempts succeeded (lock already held)")
        passed += 1
    else:
        print("✗ FAILED")
        failed += 1
    
    #test 7: transaction with mixed read/write pattern
    print_test_header(7, "Complex Read/Write Pattern")
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    pattern = [
        ('READ', 'A'), ('READ', 'B'), ('WRITE', 'A'), ('READ', 'C'),
        ('WRITE', 'B'), ('READ', 'D'), ('WRITE', 'C'), ('WRITE', 'D')
    ]
    
    results = []
    for action, obj in pattern:
        r = ccm.transaction_query(t1, TableAction.READ if action == 'READ' else TableAction.WRITE, obj)
        results.append(r.query_allowed)
    
    if all(results):
        print("✓ PASSED - Complex pattern executed successfully")
        passed += 1
    else:
        print("✗ FAILED")
        failed += 1
    
    #test 8: commit after partial execution
    print_test_header(8, "Commit After Partial Execution")
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(A), Write(B)")
    ccm.transaction_query(t1, TableAction.WRITE, 'A')
    ccm.transaction_query(t1, TableAction.WRITE, 'B')
    
    print(f"T{t2}: Write(A) - blocked/aborted")
    ccm.transaction_query(t2, TableAction.WRITE, 'A')
    
    print(f"T{t1}: Commit")
    ccm.transaction_commit(t1)
    ccm.transaction_commit_flushed(t1)
    
    print(f"T{t3}: Write(A), Write(B)")
    t3 = ccm.transaction_begin()
    r1 = ccm.transaction_query(t3, TableAction.WRITE, 'A')
    r2 = ccm.transaction_query(t3, TableAction.WRITE, 'B')
    
    if r1.query_allowed and r2.query_allowed:
        print("✓ PASSED - T3 acquired locks after T1 commit")
        passed += 1
    else:
        print("✗ FAILED")
        failed += 1
    
    #test 9: timestamp ordering verification
    print_test_header(9, "Timestamp Ordering - 10 Transactions")
    ccm = LockBasedConcurrencyControlManager()
    transactions = [ccm.transaction_begin() for _ in range(10)]
    
    timestamps = [ccm.transactions[tid]['timestamp'] for tid in transactions]
    is_ordered = all(timestamps[i] < timestamps[i+1] for i in range(len(timestamps)-1))
    
    print(f"Timestamps: {timestamps}")
    if is_ordered:
        print("✓ PASSED - Timestamps strictly increasing")
        passed += 1
    else:
        print("✗ FAILED - Timestamp ordering violated")
        failed += 1
    
    #test 10: shared lock compatibility
    print_test_header(10, "Shared Lock Compatibility - 5 Readers")
    ccm = LockBasedConcurrencyControlManager()
    readers = [ccm.transaction_begin() for _ in range(5)]
    
    results = []
    for tid in readers:
        r = ccm.transaction_query(tid, TableAction.READ, 'X')
        results.append(r.query_allowed)
    
    if all(results):
        print("✓ PASSED - All 5 readers acquired shared lock")
        passed += 1
    else:
        print("✗ FAILED")
        failed += 1
    
    #test 11: 2pl violation detection
    print_test_header(11, "2PL Violation Detection")
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(A)")
    ccm.transaction_query(t1, TableAction.WRITE, 'A')
    
    print(f"T{t1}: Commit (enters shrinking phase)")
    ccm.transaction_commit(t1)
    ccm.transaction_commit_flushed(t1)
    
    print(f"T{t1}: Write(B) - should be rejected (already terminated)")
    response = ccm.transaction_query(t1, TableAction.WRITE, 'B')
    
    if not response.query_allowed and response.status == LockStatus.FAILED:
        print(f"✓ PASSED - Transaction not active after commit: {response.reason}")
        passed += 1
    else:
        print("✗ FAILED - Should have denied invalid transaction")
        failed += 1
    
    #test 12: abort and retry pattern
    print_test_header(12, "Abort and Retry Pattern")
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(X)")
    ccm.transaction_query(t1, TableAction.WRITE, 'X')
    
    print(f"T{t2}: Write(X) - aborted (younger)")
    r1 = ccm.transaction_query(t2, TableAction.WRITE, 'X')
    t2_status = ccm.transaction_get_status(t2).value
    
    print(f"T{t1}: Commit")
    ccm.transaction_commit(t1)
    ccm.transaction_commit_flushed(t1)
    
    #create new transaction for retry
    t2_new = ccm.transaction_begin()
    print(f"T{t2_new}: Retry with new transaction")
    r2 = ccm.transaction_query(t2_new, TableAction.WRITE, 'X')
    
    if not r1.query_allowed and t2_status == 'failed' and r2.query_allowed:
        print("✓ PASSED - Retry after abort succeeded")
        passed += 1
    else:
        print("✗ FAILED")
        failed += 1
    
    #test 13: multiple lock holders with one upgrade
    print_test_header(13, "Multiple Readers with One Upgrade Attempt")
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    t3 = ccm.transaction_begin()
    
    print(f"T{t1}, T{t2}, T{t3}: All read(X)")
    ccm.transaction_query(t1, TableAction.READ, 'X')
    ccm.transaction_query(t2, TableAction.READ, 'X')
    ccm.transaction_query(t3, TableAction.READ, 'X')
    
    print(f"T{t1}: Write(X) - should wait for T2, T3")
    r = ccm.transaction_query(t1, TableAction.WRITE, 'X')
    
    if not r.query_allowed and 'wait' in r.reason.lower():
        print("✓ PASSED - Upgrade blocked by other readers")
        passed += 1
    else:
        print("✗ FAILED")
        failed += 1
    
    #test 14: sequential commits
    print_test_header(14, "Sequential Commits - Lock Reuse")
    ccm = LockBasedConcurrencyControlManager()
    
    for i in range(5):
        tid = ccm.transaction_begin()
        print(f"T{tid}: Write(X)")
        r = ccm.transaction_query(tid, TableAction.WRITE, 'X')
        if not r.query_allowed:
            print(f"✗ Iteration {i+1} failed")
            failed += 1
            break
        ccm.transaction_commit(tid)
        ccm.transaction_commit_flushed(tid)
    else:
        print("✓ PASSED - All 5 sequential transactions succeeded")
        passed += 1
    
    #test 15: wait-die with reverse age order
    print_test_header(15, "Wait-Die: Newer Requests Lock from Older")
    ccm = LockBasedConcurrencyControlManager()
    
    #create older transaction first
    t_old = ccm.transaction_begin()
    t_new = ccm.transaction_begin()
    
    print(f"T{t_old}: Write(X)")
    ccm.transaction_query(t_old, TableAction.WRITE, 'X')
    
    print(f"T{t_new}: Write(X) - younger should die")
    r = ccm.transaction_query(t_new, TableAction.WRITE, 'X')
    status = ccm.transaction_get_status(t_new).value
    
    if not r.query_allowed and status == 'failed':
        print("✓ PASSED - Younger transaction died")
        passed += 1
    else:
        print("✗ FAILED")
        failed += 1
    
    #test 16: lock release verification
    print_test_header(16, "Lock Release Verification After Abort")
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(A), Write(B), Write(C)")
    ccm.transaction_query(t1, TableAction.WRITE, 'A')
    ccm.transaction_query(t1, TableAction.WRITE, 'B')
    ccm.transaction_query(t1, TableAction.WRITE, 'C')
    
    print(f"T{t1}: Abort")
    ccm.transaction_rollback(t1)
    ccm.transaction_abort(t1)
    
    print(f"T{t2}: Write(A), Write(B), Write(C)")
    r1 = ccm.transaction_query(t2, TableAction.WRITE, 'A')
    r2 = ccm.transaction_query(t2, TableAction.WRITE, 'B')
    r3 = ccm.transaction_query(t2, TableAction.WRITE, 'C')
    
    if all([r1.query_allowed, r2.query_allowed, r3.query_allowed]):
        print("✓ PASSED - All locks released after abort")
        passed += 1
    else:
        print("✗ FAILED")
        failed += 1
    
    #test 17: mixed object types
    print_test_header(17, "Mixed Object Types (String and Int Keys)")
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    objects = ['table1', 'table2', 1, 2, 'A', 'B', 100, 200]
    results = []
    for obj in objects:
        r = ccm.transaction_query(t1, TableAction.WRITE, obj)
        results.append(r.query_allowed)
    
    if all(results):
        print(f"✓ PASSED - All {len(objects)} different object types locked")
        passed += 1
    else:
        print("✗ FAILED")
        failed += 1
    
    #test 18: stress test - 20 transactions
    print_test_header(18, "Stress Test - 20 Concurrent Transactions")
    ccm = LockBasedConcurrencyControlManager()
    transactions = [ccm.transaction_begin() for _ in range(20)]
    
    #half on object A, half on object B
    granted_a = 0
    granted_b = 0
    
    for i, tid in enumerate(transactions):
        obj = 'A' if i < 10 else 'B'
        r = ccm.transaction_query(tid, TableAction.WRITE, obj)
        if r.query_allowed:
            if obj == 'A':
                granted_a += 1
            else:
                granted_b += 1
    
    if granted_a == 1 and granted_b == 1:
        print("✓ PASSED - Correct lock distribution (1 on A, 1 on B)")
        passed += 1
    else:
        print(f"✗ FAILED - Wrong distribution ({granted_a} on A, {granted_b} on B)")
        failed += 1
    
    #test 19: read after write (same transaction)
    print_test_header(19, "Read After Write (Same Transaction)")
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    print(f"T{t1}: Write(X)")
    r1 = ccm.transaction_query(t1, TableAction.WRITE, 'X')
    
    print(f"T{t1}: Read(X)")
    r2 = ccm.transaction_query(t1, TableAction.READ, 'X')
    
    if r1.query_allowed and r2.query_allowed:
        print("✓ PASSED - Read after write in same transaction")
        passed += 1
    else:
        print("✗ FAILED")
        failed += 1
    
    #test 20: empty transaction
    print_test_header(20, "Empty Transaction (Begin and Commit Only)")
    ccm = LockBasedConcurrencyControlManager()
    t1 = ccm.transaction_begin()
    
    try:
        ccm.transaction_commit(t1)
        ccm.transaction_commit_flushed(t1)
        status = ccm.transaction_get_status(t1).value
        if status == 'committed':
            print("✓ PASSED - Empty transaction committed successfully")
            passed += 1
        else:
            print(f"✗ FAILED - Wrong status: {status}")
            failed += 1
    except Exception as e:
        print(f"✗ FAILED - Exception: {e}")
        failed += 1
    
    #summary
    print("\n" + "="*70)
    print(f"SUMMARY: {passed} passed, {failed} failed out of {passed + failed} tests")
    print(f"Success Rate: {passed/(passed+failed)*100:.1f}%")
    print("="*70)

if __name__ == "__main__":
    test_extensive_lock_based()
