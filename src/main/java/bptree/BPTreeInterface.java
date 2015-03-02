package bptree;import java.io.IOException;

/**
 * Created by max on 2/11/15.
 */
public interface BPTreeInterface {

    public boolean insert(long[] key);

    public long[][] find(long[] key);

    //public Node getRootNode();

    public long size();

}
