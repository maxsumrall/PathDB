package PageCacheSort;

import java.io.IOException;

/**
 * Created by max on 5/22/15.
 */
public interface SetIterator {

    long[] getNext() throws IOException;

    public long[] peekNext();

    public boolean hasNext();
}