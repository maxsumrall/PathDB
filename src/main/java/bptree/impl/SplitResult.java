package bptree.impl;

/**
 * Created by max on 3/19/15.
 */
public class SplitResult {
    public Long[] key = null;
    public long[] primkey = null;
    public long left;
    public long right;
    public SplitResult(Long[] k, long l, long r){key = k; left = l; right = r;}
    public SplitResult(long[] k, long l, long r){primkey = k; left = l; right = r;}
    public SplitResult(){}
}
