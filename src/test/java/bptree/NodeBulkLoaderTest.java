package bptree;

import bptree.impl.*;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by max on 5/8/15.
 */
public class NodeBulkLoaderTest {
    static Tree tree;
    NodeTree proxy;

    @Test
    public void loadTest() throws IOException {

        DiskCache disk = DiskCache.temporaryDiskCache();//TODO its bad that I make an instance of this but it has static methods. Need to refactor DiskCache.
        tree = Tree.initializeNewTree("tmp_tree_yo.dat", disk); //used for debugging
        int numberOfPages = 1000; //100000 pages should roughly equal 20mil keys;
        SimpleDataGenerator dataGenerator = new SimpleDataGenerator(numberOfPages);
        NodeBulkLoader bulkLoader = new NodeBulkLoader(dataGenerator, DiskCache.pagedFile);
        long root = bulkLoader.run();
        NodeTree proxy = new NodeTree(root, disk.getPagedFile());
        System.out.println("Done: " + root);
        for(long i = 0; i < dataGenerator.currentKey; i++){
            long[] key = new long[4];
            for (int k = 0; k < key.length; k++) {
                key[k] = i;
            }
            SearchCursor cursor = proxy.find(key);
            assert(cursor.hasNext());
        }
    }
}