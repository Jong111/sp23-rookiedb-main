package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     * <p>
     * `requestType` is guaranteed to be one of: S, X, NL.
     * <p>
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     * lock type can be, and think about how ancestor locks will need to be
     * acquired or changed.
     * <p>
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);
        LockType actualType = requestType;

        // TODO(proj4_part2): implement
        // return;
        if (effectiveLockType == LockType.NL) {
            if (requestType == LockType.NL) {
                return;
            }
            else {
                ensureAppropriateAncestorLock(transaction, lockContext, requestType);
                lockContext.acquire(transaction, requestType);
            }
        } else {
            if (requestType == LockType.NL) {
                return;
            }
            if (LockType.substitutable(effectiveLockType, requestType)) {
                return;
            }
            if (effectiveLockType.isIntent()) {
                if (effectiveLockType == LockType.IX && requestType == LockType.S) {
                    lockContext.promote(transaction, LockType.SIX);
                } else {
                    if (effectiveLockType == LockType.IS) {
                        ensureAppropriateAncestorLock(transaction, lockContext, LockType.S);
                    } else {
                        ensureAppropriateAncestorLock(transaction, lockContext, LockType.X);
                    }
                    lockContext.escalate(transaction);
                    LockType newLockType = lockContext.getExplicitLockType(transaction);
                    if (!LockType.substitutable(newLockType, requestType)) {
                        ensureAppropriateAncestorLock(transaction, lockContext, LockType.X);
                        lockContext.promote(transaction, LockType.X);
                    }
                }
            } else {
                ensureAppropriateAncestorLock(transaction, lockContext, LockType.X);
                lockContext.promote(transaction, LockType.X);
            }
        }
    }

    // TODO(proj4_part2) add any helper methods you want
    public static void ensureAppropriateAncestorLock(TransactionContext transaction, LockContext lockContext, LockType actualType) {
        assert (actualType == LockType.S || actualType == LockType.X);
        List<LockContext> ancestors = lockContext.getAncestors(transaction);
        if (actualType == LockType.S) {
            for (int i = ancestors.size() - 1; i >= 0; i--) {
                LockContext ctx = ancestors.get(i);
                Lock currLock = ctx.getLock(transaction);
                if (currLock != null) {
                    if (LockType.canBeParentLock(currLock.lockType, actualType)) {
                        continue;
                    } else {
                        ctx.promote(transaction, LockType.SIX);
                    }
                } else {
                    ctx.acquire(transaction, LockType.IS);
                }
            }
        } else {
            for (int i = ancestors.size() - 1; i >= 0; i--) {
                LockContext ctx = ancestors.get(i);
                Lock currLock = ctx.getLock(transaction);
                if (currLock != null) {
                    if (LockType.canBeParentLock(currLock.lockType, actualType)) {
                        continue;
                    } else {
                        ctx.promote(transaction, LockType.IX);
                    }
                } else {
                    ctx.acquire(transaction, LockType.IX);
                }
            }
        }
    }

    public static LockContext getRoot(TransactionContext transaction, LockContext lockContext) {
        LockContext currContext = lockContext;
        if (currContext.parent == null) {
            return currContext;
        }
        while (currContext.parent != null) {
            currContext = currContext.parentContext();
        }
        return currContext;
    }
}
