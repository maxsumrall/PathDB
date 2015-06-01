package bptree;

import bptree.impl.DiskCache;
import bptree.impl.NodeTree;
import bptree.impl.SearchCursor;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.io.pagecache.PagedFile;

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
    NodeTree tree;
    DiskCache disk;

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

        disk = DiskCache.temporaryDiskCache();
        tree = new NodeTree(0, disk);
    }

    @Test
    public void insertStuffTest() throws IOException {
        long[][] keys = new long[60000][4];
        int number_of_paths = 10000;
        for (int i = 0; i < keys.length; i++) {
            keys[i][0] = (long)(i % number_of_paths);
            keys[i][1] = (long)i;
            keys[i][2] = (long)i;
            keys[i][3] = (long)i;
            tree.insert(keys[i]);
        }
        SearchCursor searchCursor;
        long[] foundKey;
        for(long[] key : keys){
            searchCursor = tree.find(key);
            try(PageProxyCursor cursor = disk.getCursor(searchCursor.pageID, PagedFile.PF_EXCLUSIVE_LOCK)) {
                foundKey = searchCursor.next(cursor);
                assert (Arrays.equals(foundKey, key));
            }
        }
        for(int i = 0; i < number_of_paths; i++){
            int count = 0;
            searchCursor = tree.find(new long[]{i});
            try(PageProxyCursor cursor = disk.getCursor(searchCursor.pageID, PagedFile.PF_EXCLUSIVE_LOCK)) {
                while (searchCursor.hasNext(cursor)) {
                    long[] next = searchCursor.next(cursor);
                    count++;
                }
            }
            assert(count == keys.length / number_of_paths);
        }


        for(int i = 0; i < keys.length/2; i++){
            tree.remove(keys[i]);
            }
        for(int i = keys.length/2; i < keys.length; i++){
            //assert(pindex.tree.proxyFind(keys[i]).hasNext());
        }
        for(int i = 0; i < keys.length/2; i++){
           // assert(!pindex.tree.proxyFind(keys[i]).hasNext());
        }
    }
}
