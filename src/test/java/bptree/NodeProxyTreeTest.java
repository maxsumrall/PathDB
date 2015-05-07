package bptree;

import bptree.impl.NodeProxy;
import bptree.impl.PathIndexImpl;
import bptree.impl.ProxyCursor;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static bptree.LabelsAndPathsGenerator.exampleLabelPaths;

/**
 * Created by max on 5/1/15.
 */
public class NodeProxyTreeTest {

    Index index;
    ArrayList<Long[]> labelPaths;
    PathIndexImpl pindex;
    NodeProxy proxy;

    public long[] toPrimitive(Long[] key) {
        long[] keyprim = new long[key.length];
        for (int i = 0; i < key.length; i++) {
            keyprim[i] = key[i];
        }
        return keyprim;
    }

    @Before
    public void initializeIndex() throws IOException {
        labelPaths = exampleLabelPaths(2, 2);
        index = PathIndexImpl.getTemporaryPathIndex()
                .setRangeOfPathLengths(2, 2)
                .setLabelPaths(labelPaths)
                .setSignaturesToDefault();

        pindex = ((PathIndexImpl) index);
        proxy = new NodeProxy(pindex.tree.rootNodePageID, pindex.tree.nodeKeeper.diskCache.pagedFile);
    }

    @Test
    public void insertStuffTest() throws IOException {
        long[][] keys = new long[120000][4];
        int number_of_paths = 10000;
        for (int i = 0; i < keys.length; i++) {
            keys[i][0] = (long)(i % number_of_paths);
            keys[i][1] = (long)i;
            keys[i][2] = (long)i;
            keys[i][3] = (long)i;
            pindex.tree.proxyInsertion(keys[i]);
        }
        ProxyCursor cursor;
        long[] foundKey;
        for(long[] key : keys){
            cursor = pindex.tree.proxyFind(key);
            foundKey = cursor.next();
            assert(Arrays.equals(foundKey, key));
        }
        for(int i = 0; i < number_of_paths; i++){
            int count = 0;
            cursor = pindex.tree.proxyFind(new long[]{i});
            while(cursor.hasNext()){
                long[] next = cursor.next();
                count++;
            }
            assert(count == keys.length / number_of_paths);
        }
    }
}
