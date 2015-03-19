package bptree;

/**
 * Created by max on 3/19/15.
 */
public class SplitResult {
    public final Long[] key;
    public final long left;
    public final long right;
    public SplitResult(Long[] k, long l, long r){key = k; left = l; right = r;}
}
