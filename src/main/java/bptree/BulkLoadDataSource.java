package bptree;

import java.io.IOException;

/**
 * Created by max on 5/8/15.
 */
public interface BulkLoadDataSource {

    byte[] nextPage() throws IOException;
    boolean hasNext() throws IOException;
}
