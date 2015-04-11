package bptree;

import java.io.IOException;
import java.util.List;

/**
 * Created by max on 3/26/15.
 */
public interface Index {

    Index setRangeOfPathLengths(int minimumPathLength, int maximumPathLength);

    Index setLabelPaths(List<Long[]> labelPaths);

    Index setSignatures(List<Integer[]> signatures);

    Index setSignaturesToDefault();

    List<Integer[]> getDefaultSignatures();

    Long[] buildComposedKey(Key key);

    Cursor find(Key key) throws IOException;

    void insert(Key key) throws IOException;

    boolean remove(Key key) throws IOException;

    void close() throws IOException;
}
