package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     * <p>
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException          if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     *                                       transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        if (this.readonly) {
            throw new UnsupportedOperationException("this context is readonly");
        }

        Lock lock = getLock(transaction);
        if (lock != null) {
            throw new DuplicateLockRequestException("duplicate lock request");
        }

        boolean valid = true;
        List<LockContext> ancestors = getAncestors(transaction);
        for (LockContext ctx : ancestors) {
            Lock lockOnThisAncestor = ctx.getLock(transaction);
            if (lockOnThisAncestor != null) {
                if (!LockType.canBeParentLock(lockOnThisAncestor.lockType, lockType)) {
                    if (lockOnThisAncestor.lockType != LockType.NL) {
                        valid = false;
                        break;
                    } else {
                        valid = lockType.isIntent();
                    }
                }
            }
        }
        if (!valid) {
            throw new InvalidLockException("invalid lock request");
        }

        lockman.acquire(transaction, this.name, lockType);
        long transactionNum = transaction.getTransNum();
        for (LockContext ctx : ancestors) {
            if (ctx.numChildLocks.containsKey(transactionNum)) {
                ctx.numChildLocks.put(transactionNum, ctx.numChildLocks.get(transactionNum) + 1);
            } else {
                ctx.numChildLocks.put(transactionNum, 1);
            }
        }

        // return;
    }

    /**
     * Release `transaction`'s lock on `name`.
     * <p>
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException           if no lock on `name` is held by `transaction`
     * @throws InvalidLockException          if the lock cannot be released because
     *                                       doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        Lock lock = getLock(transaction);
        if (this.readonly) {
            throw new UnsupportedOperationException("this context is readonly");
        }

        if (lock == null) {
            throw new NoLockHeldException("no lock on name is held by transaction");
        }

        boolean valid = true;
        if (lock.lockType.isIntent()) {
            List<LockContext> descendants = getDescendant(transaction);
            for (LockContext ctx : descendants) {
                Lock lockOnThisDescendant = ctx.getLock(transaction);
                if (lockOnThisDescendant != null) {
                    if (lockOnThisDescendant.lockType == LockType.S) {
                        valid = !(lock.lockType == LockType.IS || lock.lockType == LockType.IX);
                    } else if (lockOnThisDescendant.lockType == LockType.X) {
                        valid = !(lock.lockType == LockType.IX || lock.lockType == LockType.SIX);
                    }
                }
            }
        }
        if (!valid) {
            throw new InvalidLockException("invalid lock request");
        }

        lockman.release(transaction, this.name);
        long transactionNum = transaction.getTransNum();
        List<LockContext> ancestors = getAncestors(transaction);
        for (LockContext ctx : ancestors) {
            ctx.numChildLocks.put(transactionNum, ctx.numChildLocks.get(transactionNum) - 1);
        }
        // return;
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     * <p>
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     *                                       `newLockType` lock
     * @throws NoLockHeldException           if `transaction` has no lock
     * @throws InvalidLockException          if the requested lock type is not a
     *                                       promotion or promoting would cause the lock manager to enter an invalid
     *                                       state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     *                                       type B is valid if B is substitutable for A and B is not equal to A, or
     *                                       if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     *                                       be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        Lock lock = getLock(transaction);
        if (lock == null) {
            throw new NoLockHeldException("the transaction has no lock");
        }

        if (lock != null && lock.lockType == newLockType) {
            throw new DuplicateLockRequestException("duplicate lock request");
        }

        boolean isPromotion = (LockType.substitutable(newLockType, lock.lockType) && (newLockType != lock.lockType));
        if(!isPromotion) {
            throw new InvalidLockException("invalid promote request");
        }
        boolean valid = true;
        List<LockContext> ancestors = getAncestors(transaction);
        for (LockContext ctx : ancestors) {
            Lock lockOnThisAncestor = ctx.getLock(transaction);
            if (lockOnThisAncestor != null) {
                if (!LockType.canBeParentLock(lockOnThisAncestor.lockType, newLockType)) {
                    if (lockOnThisAncestor.lockType != LockType.NL) {
                        if(!newLockType.isIntent()) {
                            valid = false;
                            break;
                        }
                    }
                }
            }
        }
        if (!valid || !isPromotion) {
            int ttt = 0;
            throw new InvalidLockException("invalid promote request");
        }

        if ((lock.lockType == LockType.IS || lock.lockType == LockType.IX) && newLockType == LockType.SIX) {
            List<ResourceName> sisResources = sisDescendants(transaction);
            for (ResourceName resource : sisResources) {
                LockContext currContext = fromResourceName(lockman, resource);
                currContext.release(transaction);
            }
        }
        lockman.promote(transaction, this.name, newLockType);
        // return;
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     * <p>
     * For example, if a transaction has the following locks:
     * <p>
     * IX(database)
     * /         \
     * IX(table1)    S(table2)
     * /      \
     * S(table1 page3)  X(table1 page5)
     * <p>
     * then after table1Context.escalate(transaction) is called, we should have:
     * <p>
     * IX(database)
     * /         \
     * X(table1)     S(table2)
     * <p>
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     * <p>
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException           if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        Lock lock = getLock(transaction);
        if (lock == null) {
            throw new NoLockHeldException("transaction has no lock at this level");
        }

        if (this.readonly) {
            throw new UnsupportedOperationException("the context is readonly");
        }

        List<LockContext> descendants = getDescendant(transaction);
        List<Lock> locks = lockman.getLocks(transaction);
        boolean canUseS = true;
        for (LockContext ctx : descendants) {
            Lock currLock = ctx.getLock(transaction);
            if (currLock != null) {
                if (!LockType.substitutable(LockType.S, currLock.lockType)) {
                    canUseS = false;
                }
                ctx.release(transaction);
            }
        }
        if (canUseS) {
            if (lock.lockType != LockType.S)
                canUseS = LockType.substitutable(LockType.S, lock.lockType);
        }
        if (canUseS) {
            if (lock.lockType != LockType.S) {
                promote(transaction, LockType.S);
            }
        } else {
            if (lock.lockType != LockType.X) {
                promote(transaction, LockType.X);
            }
        }
        // return;
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        Lock lock = getLock(transaction);
        if (lock != null) {
            return lock.lockType;
        } else {
            return LockType.NL;
        }
        // return LockType.NL;
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        LockType lockType = getExplicitLockType(transaction);
        if (lockType != LockType.NL) {
            return lockType;
        } else {
            LockContext currContext = this;
            while (currContext.parent != null) {
                currContext = currContext.parentContext();
                Lock currLock = currContext.getLock(transaction);
                if (currLock != null) {
                    LockType currLockType = currLock.lockType;
                    if (currLockType == LockType.SIX) {
                        return LockType.S;
                    }
                    if (!currLockType.isIntent() && currLockType != LockType.NL) {
                        return currLockType;
                    }
                }
            }
            return LockType.NL;
        }
        // return LockType.NL;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     *
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        // return false;
        LockContext currContext = this;
        if (currContext.parent == null) {
            return false;
        }
        List<Lock> locks = lockman.getLocks(transaction);
        while (currContext.parent != null) {
            currContext = currContext.parentContext();
            Lock currLock = currContext.getLock(transaction);
            if (currLock.lockType == LockType.SIX) {
                return true;
            }
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     *
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        // return new ArrayList<>();
        List<ResourceName> res = new ArrayList<>();
        List<Lock> locks = lockman.getLocks(transaction);
        for (Lock lk : locks) {
            ResourceName resource = lk.name;
            if (resource.isDescendantOf(this.name)) {
                if (lk.lockType == LockType.S || lk.lockType == LockType.IS) {
                    res.add(resource);
                }
            }
        }
        return res;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }

    /**
     * Helper method to return the Lock held by transaction on the resource
     * represented by this LockContext
     */
    public Lock getLock(TransactionContext transaction) {
        List<Lock> locks = lockman.getLocks(transaction);
        for (Lock lk : locks) {
            if (lk.name.equals(this.name)) {
                return lk;
            }
        }
        return null;
    }

    /**
     * Helper method to get the ancestors of this LockContext object
     */
    public List<LockContext> getAncestors(TransactionContext transaction) {
        List<LockContext> res = new ArrayList<>();
        LockContext currContext = this;
        while (currContext.parent != null) {
            currContext = currContext.parentContext();
            LockContext tmp = currContext;
            res.add(tmp);
        }
        // int tt = 0;
        return res;
    }

    /**
     * Helper method to get the descendants of this LockContext object
     */
    public List<LockContext> getDescendant(TransactionContext transaction) {
        List<LockContext> res = new ArrayList<>();
        List<Lock> locks = lockman.getLocks(transaction);
        for (Lock lk : locks) {
            ResourceName resource = lk.name;
            if (resource.isDescendantOf(this.name)) {
                LockContext tmpContext = fromResourceName(lockman, resource);
                res.add(tmpContext);
            }
        }
        return res;
    }
}

