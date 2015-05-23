package bptree;

import bptree.impl.*;
import org.junit.Test;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;

/**
 * Created by max on 5/8/15.
 */
public class NodeBulkLoaderTest {
    static Tree tree;
    NodeTree proxy;

    @Test
    public void loadTest() throws IOException {

        DiskCache disk = DiskCache.temporaryDiskCache();
        tree = Tree.initializeNewTree("tmp_tree_yo.dat", disk); //used for debugging
        int numberOfPages = 1000; //100000 pages should roughly equal 20mil keys;
        SimpleDataGenerator dataGenerator = new SimpleDataGenerator(numberOfPages);
        NodeBulkLoader bulkLoader = new NodeBulkLoader(dataGenerator, disk);
        long root = bulkLoader.run();
        NodeTree proxy = new NodeTree(root, disk.getPagedFile());
        System.out.println("Done: " + root);
        for(long i = 0; i < dataGenerator.currentKey; i++){
            long[] key = new long[4];
            for (int k = 0; k < key.length; k++) {
                key[k] = i;
            }
            SearchCursor searchCursor = proxy.find(key);
            try(PageProxyCursor cursor = disk.getCursor(searchCursor.pageID, PagedFile.PF_EXCLUSIVE_LOCK)){
                assert(searchCursor.hasNext(cursor));
            }
        }
    }
}