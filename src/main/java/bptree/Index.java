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

    Key buildKey(Long[] relationships, Long[] nodes);

    Long[] buildComposedKey(Key key);

    Cursor find(Key key);

    void insert(Key key);

    RemoveResult remove(Key key);

    void shutdown() throws IOException;

    void close() throws IOException;
}
