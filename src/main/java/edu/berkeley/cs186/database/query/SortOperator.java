package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.PageDirectory;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.*;

public class SortOperator extends QueryOperator {
    protected Comparator<Record> comparator;
    private TransactionContext transaction;
    private Run sortedRecords;
    private int numBuffers;
    private int sortColumnIndex;
    private String sortColumnName;

    public SortOperator(TransactionContext transaction, QueryOperator source,
                        String columnName) {
        super(OperatorType.SORT, source);
        this.transaction = transaction;
        this.numBuffers = this.transaction.getWorkMemSize();
        this.sortColumnIndex = getSchema().findField(columnName);
        this.sortColumnName = getSchema().getFieldName(this.sortColumnIndex);
        this.comparator = new RecordComparator();
    }

    private class RecordComparator implements Comparator<Record> {
        @Override
        public int compare(Record r1, Record r2) {
            return r1.getValue(sortColumnIndex).compareTo(r2.getValue(sortColumnIndex));
        }
    }

    @Override
    public TableStats estimateStats() {
        return getSource().estimateStats();
    }

    @Override
    public Schema computeSchema() {
        return getSource().getSchema();
    }

    @Override
    public int estimateIOCost() {
        int N = getSource().estimateStats().getNumPages();
        double pass0Runs = Math.ceil(N / (double) numBuffers);
        double numPasses = 1 + Math.ceil(Math.log(pass0Runs) / Math.log(numBuffers - 1));
        return (int) (2 * N * numPasses) + getSource().estimateIOCost();
    }

    @Override
    public String str() {
        return "Sort (cost=" + estimateIOCost() + ")";
    }

    @Override
    public List<String> sortedBy() {
        return Collections.singletonList(sortColumnName);
    }

    @Override
    public boolean materialized() {
        return true;
    }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (this.sortedRecords == null) this.sortedRecords = sort();
        return sortedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * Returns a Run containing records from the input iterator in sorted order.
     * You're free to use an in memory sort over all the records using one of
     * Java's built-in sorting methods.
     *
     * @return a single sorted run containing all the records from the input
     * iterator
     */
    public Run sortRun(Iterator<Record> records) {
        // TODO(proj3_part1): implement
        // return null;
        List<Record> recordList = new ArrayList<>();
        while (records.hasNext()) {
            recordList.add(records.next());
        }
        Collections.sort(recordList, new RecordComparator());
        return makeRun(recordList);
    }

    /**
     * Given a list of sorted runs, returns a new run that is the result of
     * merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run
     * next.
     * <p>
     * You are NOT allowed to have more than runs.size() records in your
     * priority queue at a given moment. It is recommended that your Priority
     * Queue hold Pair<Record, Integer> objects where a Pair (r, i) is the
     * Record r with the smallest value you are sorting on currently unmerged
     * from run i. `i` can be useful to locate which record to add to the queue
     * next after the smallest element is removed.
     *
     * @return a single sorted run obtained by merging the input runs
     */
    public Run mergeSortedRuns(List<Run> runs) {
        assert (runs.size() <= this.numBuffers - 1);
        // TODO(proj3_part1): implement
        // return null;
        int n = runs.size();
        List<Record> recordList = new ArrayList<>();
        List<BacktrackingIterator<Record>> iters = new ArrayList<>();
        PriorityQueue<Pair<Record, Integer>> bufferQueue = new PriorityQueue<>(new RecordPairComparator());
        for (int i = 0; i < n; i++) {
            BacktrackingIterator<Record> iter = runs.get(i).iterator();
            iters.add(iter);
        }
        for (int i = 0; i < n; i++) {
            assert (iters.get(i).hasNext());
            bufferQueue.offer(new Pair<Record, Integer>(iters.get(i).next(), i));
        }
        while (!bufferQueue.isEmpty()) {
            Pair<Record, Integer> targetPair = bufferQueue.poll();
            recordList.add(targetPair.getFirst());
            if (iters.get(targetPair.getSecond()).hasNext()) {
                bufferQueue.offer(new Pair<Record, Integer>(iters.get(targetPair.getSecond()).next(), targetPair.getSecond()));
            }
        }
        return makeRun(recordList);
    }

    /**
     * Compares the two (record, integer) pairs based only on the record
     * component using the default comparator. You may find this useful for
     * implementing mergeSortedRuns.
     */
    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }

    /**
     * Given a list of N sorted runs, returns a list of sorted runs that is the
     * result of merging (numBuffers - 1) of the input runs at a time. If N is
     * not a perfect multiple of (numBuffers - 1) the last sorted run should be
     * the result of merging less than (numBuffers - 1) runs.
     *
     * @return a list of sorted runs obtained by merging the input runs
     */
    public List<Run> mergePass(List<Run> runs) {
        // TODO(proj3_part1): implement
        // return Collections.emptyList();
        List<Run> res = new ArrayList<>();
        int N = runs.size();
        int n = N;
        if (N >= this.numBuffers - 1) {
            int l = 0;
            int r = this.numBuffers - 1;
            int isPerfectMultiple = 0;
            while (n >= this.numBuffers - 1) {
                List<Run> currentRuns = runs.subList(l, r);
                Run currentMergedRun = mergeSortedRuns(currentRuns);
                res.add(currentMergedRun);
                n = n - (this.numBuffers - 1);
                if (n == 0) {
                    isPerfectMultiple = 1;
                    break;
                }
                l += (this.numBuffers - 1);
                if (r + this.numBuffers - 1 <= N) {
                    r += (this.numBuffers - 1);
                } else {
                    r = N;
                }
            }
            if (isPerfectMultiple == 0) {
                List<Run> currentRuns = runs.subList(l, r);
                Run currentMergedRun = mergeSortedRuns(currentRuns);
                res.add(currentMergedRun);
            }
            return res;
        } else {
            Run currentMergedRun = mergeSortedRuns(runs);
            res.add(currentMergedRun);
            return res;
        }
    }

    /**
     * Does an external merge sort over the records of the source operator.
     * You may find the getBlockIterator method of the QueryOperator class useful
     * here to create your initial set of sorted runs.
     *
     * @return a single run containing all of the source operator's records in
     * sorted order.
     */
    public Run sort() {
        // Iterator over the records of the relation we want to sort
        Iterator<Record> sourceIterator = getSource().iterator();

        // TODO(proj3_part1): implement
        // return makeRun(); // TODO(proj3_part1): replace this!
        List<Run> runs = new ArrayList<>();
        while (sourceIterator.hasNext()) {
            BacktrackingIterator<Record> iter = QueryOperator.getBlockIterator(sourceIterator, getSource().getSchema(), this.numBuffers);
            Run run = sortRun(iter);
            runs.add(run);
        }
        int n = runs.size();
        if (n == 1) {
            return runs.get(0);
        }
        if (n > this.numBuffers - 1) {
            List<Run> updatedRuns = mergePass(runs);
            n = updatedRuns.size();
            while (n > this.numBuffers - 1) {
                updatedRuns = mergePass(updatedRuns);
                n = updatedRuns.size();
            }
            return mergeSortedRuns(updatedRuns);
        } else {
            return mergeSortedRuns(runs);
        }
    }

    /**
     * @return a new empty run.
     */
    public Run makeRun() {
        return new Run(this.transaction, getSchema());
    }

    /**
     * @param records
     * @return A new run containing the records in `records`
     */
    public Run makeRun(List<Record> records) {
        Run run = new Run(this.transaction, getSchema());
        run.addAll(records);
        return run;
    }
}

