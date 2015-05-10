package bptree;

/**
 * Created by max on 5/8/15.
 */
public interface BulkLoadDataSource {

    byte[] nextPage();
    boolean hasNext();
}
