"""
Unit Test untuk Lock-Based Concurrency Control Manager
Menggunakan protokol 2PL (Two-Phase Locking) dengan Wait-Die untuk deadlock prevention
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.lock_based_concurrency_control_manager import LockBasedConcurrencyControlManager
from src.row_action import TableAction
from src.concurrency_response import LockStatus


def test_1_concurrent_read_write_conflict():
    """
    TEST 1: Concurrent Read-Write Conflict dengan Wait-Die Protocol
    
    Description
    -----------
    Menguji bagaimana concurrency control menangani konflik antara transaksi yang 
    melakukan READ dan WRITE pada resource yang sama, menggunakan Wait-Die protocol
    untuk mencegah deadlock.
    
    Goal
    ----
    1. Memverifikasi bahwa shared lock (READ) dapat dipegang oleh satu transaksi
    2. Menguji bahwa exclusive lock (WRITE) tidak dapat dipegang bersamaan dengan shared lock
    3. Memvalidasi Wait-Die protocol: transaksi yang lebih muda harus abort (die)
    4. Memastikan blocked_by field berisi informasi transaksi yang memblokir
    5. Memverifikasi active_transactions list mencerminkan status transaksi
    
    Method
    ------
    1. Inisialisasi Lock-Based CCM
    2. Buat dua transaksi (T1 dan T2, dimana T2 lebih muda)
    3. T1 melakukan READ pada resource X (dapat shared lock)
    4. T2 mencoba WRITE pada resource X (konflik dengan T1)
    5. Verifikasi response dari T2:
       - Status harus FAILED (karena T2 lebih muda)
       - blocked_by harus berisi [T1]
       - active_transactions harus berisi T1 (T2 sudah abort)
    6. Verifikasi T1 masih memegang lock (dapat READ lagi)
    
    Success Criterion
    -----------------
    1. T1 berhasil mendapat READ lock (status GRANTED)
    2. T2 gagal mendapat WRITE lock (status FAILED, bukan WAITING)
    3. blocked_by pada response T2 berisi transaction ID T1
    4. active_transactions tidak berisi T2 (sudah abort)
    5. T1 masih dapat melakukan operasi (belum abort)
    6. Response reason menjelaskan Wait-Die abort
    
    Input
    -----
    - T1: READ pada resource 'X'
    - T2: WRITE pada resource 'X' (konflik dengan T1)
    
    Expected Output
    ---------------
    Response T1 READ:
      - status: GRANTED
      - blocked_by: []
      - active_transactions: [1, 2]
    
    Response T2 WRITE:
      - status: FAILED
      - blocked_by: [1]
      - active_transactions: [1] (T2 sudah abort)
      - reason: "Transaction 2 (TS=2) aborted by Wait-Die (younger than holder 1 TS=1)"
    
    Response T1 READ kedua:
      - status: GRANTED (masih memegang lock)
    """
    print("\n" + "="*80)
    print("TEST 1: Concurrent Read-Write Conflict dengan Wait-Die Protocol")
    print("="*80)
    
    # 1. Inisialisasi CCM
    ccm = LockBasedConcurrencyControlManager()
    
    # 2. Buat dua transaksi
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    print(f"Created transactions: T{t1} (TS=1) and T{t2} (TS=2)")
    
    # 3. T1 melakukan READ pada X
    print(f"\nT{t1}: Attempting READ on 'X'")
    r1 = ccm.transaction_query(t1, TableAction.READ, 'X')
    print(f"  Status: {r1.status.value}")
    print(f"  Reason: {r1.reason}")
    print(f"  Blocked by: {r1.blocked_by}")
    print(f"  Active transactions: {r1.active_transactions}")
    
    # Verifikasi T1 READ berhasil
    assert r1.status == LockStatus.GRANTED, "T1 READ should be GRANTED"
    assert r1.blocked_by == [], "T1 should not be blocked"
    assert t1 in r1.active_transactions, "T1 should be in active transactions"
    assert t2 in r1.active_transactions, "T2 should be in active transactions"
    print("  ✓ T1 successfully acquired READ lock")
    
    # 4. T2 mencoba WRITE pada X (konflik)
    print(f"\nT{t2}: Attempting WRITE on 'X' (conflict with T{t1})")
    r2 = ccm.transaction_query(t2, TableAction.WRITE, 'X')
    print(f"  Status: {r2.status.value}")
    print(f"  Reason: {r2.reason}")
    print(f"  Blocked by: {r2.blocked_by}")
    print(f"  Active transactions: {r2.active_transactions}")
    
    # 5. Verifikasi response T2
    assert r2.status == LockStatus.FAILED, "T2 WRITE should FAIL (younger transaction)"
    assert r2.blocked_by == [t1], f"T2 should be blocked by T{t1}"
    assert t2 not in r2.active_transactions, "T2 should be aborted (not in active)"
    assert t1 in r2.active_transactions, "T1 should still be active"
    assert "Wait-Die" in r2.reason, "Reason should mention Wait-Die"
    assert "aborted" in r2.reason.lower(), "Reason should mention abort"
    print(f"  ✓ T2 correctly aborted by Wait-Die (younger than T{t1})")
    
    # 6. Verifikasi T1 masih memegang lock
    print(f"\nT{t1}: Attempting READ on 'X' again (should still hold lock)")
    r3 = ccm.transaction_query(t1, TableAction.READ, 'X')
    print(f"  Status: {r3.status.value}")
    
    assert r3.status == LockStatus.GRANTED, "T1 should still have lock"
    print("  ✓ T1 still holds the lock")
    
    print("\n" + "="*80)
    print("TEST 1: PASSED ✓")
    print("="*80)
    print("Summary:")
    print("- T1 (older) successfully held READ lock throughout")
    print("- T2 (younger) was aborted by Wait-Die protocol")
    print("- blocked_by correctly identified blocking transaction")
    print("- active_transactions correctly reflected transaction states")
    print("="*80)


def test_2_lock_upgrade_with_waiting():
    """
    TEST 2: Lock Upgrade dari Shared ke Exclusive dengan Waiting
    
    Description
    -----------
    Menguji kemampuan concurrency control untuk melakukan lock upgrade (dari shared
    lock menjadi exclusive lock) dan menangani situasi dimana transaksi harus menunggu
    karena ada transaksi lain yang memegang shared lock pada resource yang sama.
    
    Goal
    ----
    1. Memverifikasi bahwa multiple shared locks dapat dipegang secara bersamaan
    2. Menguji lock upgrade dari READ ke WRITE pada transaksi yang sama
    3. Memvalidasi Wait-Die protocol untuk lock upgrade: transaksi yang lebih tua WAITS
    4. Memastikan blocked_by berisi semua transaksi yang memegang shared lock
    5. Memverifikasi status WAITING (bukan FAILED) untuk transaksi yang menunggu
    6. Menguji bahwa lock berhasil di-upgrade setelah blocker commit
    
    Method
    ------
    1. Inisialisasi Lock-Based CCM
    2. Buat tiga transaksi (T1, T2, T3)
    3. T1, T2, T3 semua melakukan READ pada resource Y (shared locks)
    4. T1 mencoba upgrade ke WRITE (harus wait karena T2 dan T3 masih hold)
    5. Verifikasi response T1:
       - Status harus WAITING (T1 lebih tua, jadi wait)
       - blocked_by harus berisi [T2, T3]
       - should_retry harus True
    6. T2 commit dan release lock
    7. T1 masih harus wait (T3 masih hold)
    8. T3 commit dan release lock
    9. T1 mencoba WRITE lagi, sekarang harus berhasil (upgrade completed)
    
    Success Criterion
    -----------------
    1. T1, T2, T3 semua berhasil mendapat READ lock (concurrent reads)
    2. T1 upgrade attempt menghasilkan WAITING (bukan FAILED)
    3. blocked_by berisi [T2, T3] (atau [T3, T2], order tidak penting)
    4. should_retry property bernilai True untuk response WAITING
    5. Setelah T2 dan T3 commit, T1 dapat melakukan WRITE
    6. Final WRITE lock status adalah GRANTED
    
    Input
    -----
    - T1, T2, T3: semua READ pada resource 'Y'
    - T1: upgrade WRITE pada resource 'Y' (while T2, T3 still hold)
    - T2: commit
    - T3: commit
    - T1: retry WRITE pada resource 'Y'
    
    Expected Output
    ---------------
    Initial READs (T1, T2, T3):
      - status: GRANTED untuk semua
      - active_transactions: [1, 2, 3]
    
    T1 WRITE attempt 1:
      - status: WAITING
      - blocked_by: [2, 3] atau [3, 2]
      - should_retry: True
      - reason: "shared locks held by 2 transaction(s)"
    
    After T2, T3 commit:
      - T1 WRITE succeeds
      - status: GRANTED
      - Lock successfully upgraded
    """
    print("\n" + "="*80)
    print("TEST 2: Lock Upgrade dengan Waiting untuk Multiple Shared Locks")
    print("="*80)
    
    # 1. Inisialisasi CCM
    ccm = LockBasedConcurrencyControlManager()
    
    # 2. Buat tiga transaksi
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    t3 = ccm.transaction_begin()
    print(f"Created transactions: T{t1} (TS=1), T{t2} (TS=2), T{t3} (TS=3)")
    
    # 3. Semua transaksi melakukan READ pada Y
    print(f"\nT{t1}, T{t2}, T{t3}: All acquiring READ locks on 'Y'")
    r1_read = ccm.transaction_query(t1, TableAction.READ, 'Y')
    r2_read = ccm.transaction_query(t2, TableAction.READ, 'Y')
    r3_read = ccm.transaction_query(t3, TableAction.READ, 'Y')
    
    print(f"  T{t1} READ: {r1_read.status.value}")
    print(f"  T{t2} READ: {r2_read.status.value}")
    print(f"  T{t3} READ: {r3_read.status.value}")
    
    # Verifikasi semua mendapat shared lock
    assert r1_read.status == LockStatus.GRANTED, "T1 READ should be GRANTED"
    assert r2_read.status == LockStatus.GRANTED, "T2 READ should be GRANTED"
    assert r3_read.status == LockStatus.GRANTED, "T3 READ should be GRANTED"
    print("  ✓ All transactions successfully acquired shared locks")
    
    # 4. T1 mencoba upgrade ke WRITE
    print(f"\nT{t1}: Attempting WRITE on 'Y' (lock upgrade)")
    r1_write = ccm.transaction_query(t1, TableAction.WRITE, 'Y')
    print(f"  Status: {r1_write.status.value}")
    print(f"  Reason: {r1_write.reason}")
    print(f"  Blocked by: {r1_write.blocked_by}")
    print(f"  Should retry: {r1_write.should_retry}")
    
    # 5. Verifikasi T1 harus WAIT (bukan FAIL karena T1 lebih tua)
    assert r1_write.status == LockStatus.WAITING, "T1 should WAIT (older transaction)"
    assert set(r1_write.blocked_by) == {t2, t3}, f"T1 should be blocked by T{t2} and T{t3}"
    assert r1_write.should_retry == True, "should_retry should be True for WAITING"
    assert "shared locks" in r1_write.reason.lower(), "Reason should mention shared locks"
    print(f"  ✓ T1 correctly waiting for T{t2} and T{t3} to release")
    
    # 6. T2 commit (release lock)
    print(f"\nT{t2}: Committing (releasing locks)")
    ccm.transaction_commit(t2)
    ccm.transaction_commit_flushed(t2)
    print(f"  ✓ T{t2} released locks")
    
    # 7. T1 masih harus wait (T3 masih memegang lock)
    print(f"\nT{t1}: Attempting WRITE on 'Y' again (T{t3} still holds lock)")
    r1_write2 = ccm.transaction_query(t1, TableAction.WRITE, 'Y')
    print(f"  Status: {r1_write2.status.value}")
    print(f"  Blocked by: {r1_write2.blocked_by}")
    
    assert r1_write2.status == LockStatus.WAITING, "T1 should still WAIT (T3 holds lock)"
    assert r1_write2.blocked_by == [t3], f"T1 should be blocked only by T{t3} now"
    print(f"  ✓ T1 still waiting (blocked by T{t3})")
    
    # 8. T3 commit (release lock)
    print(f"\nT{t3}: Committing (releasing locks)")
    ccm.transaction_commit(t3)
    ccm.transaction_commit_flushed(t3)
    print(f"  ✓ T{t3} released locks")
    
    # 9. T1 sekarang harus berhasil upgrade
    print(f"\nT{t1}: Attempting WRITE on 'Y' (all blockers released)")
    r1_write3 = ccm.transaction_query(t1, TableAction.WRITE, 'Y')
    print(f"  Status: {r1_write3.status.value}")
    print(f"  Reason: {r1_write3.reason}")
    print(f"  Blocked by: {r1_write3.blocked_by}")
    
    assert r1_write3.status == LockStatus.GRANTED, "T1 WRITE should be GRANTED (upgrade successful)"
    assert r1_write3.blocked_by == [], "T1 should not be blocked"
    assert "exclusive" in r1_write3.reason.lower(), "Should have exclusive lock"
    print(f"  ✓ T1 successfully upgraded to exclusive lock")
    
    # Cleanup
    ccm.transaction_commit(t1)
    ccm.transaction_commit_flushed(t1)
    
    print("\n" + "="*80)
    print("TEST 2: PASSED ✓")
    print("="*80)
    print("Summary:")
    print("- Multiple shared locks acquired successfully (T1, T2, T3)")
    print("- T1 correctly waited for other shared lock holders")
    print("- blocked_by dynamically updated as transactions committed")
    print("- Lock upgrade completed after all blockers released")
    print("- Wait-Die protocol: older transaction (T1) waited, not aborted")
    print("="*80)


if __name__ == "__main__":
    print("\n" + "="*80)
    print("LOCK-BASED CONCURRENCY CONTROL - UNIT TESTS")
    print("Protocol: Two-Phase Locking (2PL) + Wait-Die Deadlock Prevention")
    print("="*80)
    
    try:
        test_1_concurrent_read_write_conflict()
        test_2_lock_upgrade_with_waiting()
        
        print("\n" + "="*80)
        print("ALL TESTS PASSED ✓✓")
        print("="*80)
        print("Results:")
        print("  Test 1: Concurrent Read-Write Conflict - PASSED")
        print("  Test 2: Lock Upgrade with Waiting - PASSED")
        print("\nConcurrency control is working correctly!")
        print("="*80)
        
    except AssertionError as e:
        print(f"\n✗ TEST FAILED: {e}")
        raise
    except Exception as e:
        print(f"\n✗ UNEXPECTED ERROR: {e}")
        raise
