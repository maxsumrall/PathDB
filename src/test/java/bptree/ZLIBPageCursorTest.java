package bptree;

import bptree.impl.*;
import org.junit.Test;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;

/**
 * Created by max on 5/11/15.
 */
public class ZLIBPageCursorTest {

    Tree tree;

    @Test
    public void loadTest() throws IOException {

        DiskCache disk = DiskCache.temporaryDiskCache();//TODO its bad that I make an instance of this but it has static methods. Need to refactor DiskCache.
        tree = Tree.initializeNewTree("tmp_tree_yo.dat", disk); //used for debugging
        int numberOfPages = 1000; //100000 pages should roughly equal 20mil keys;
        SimpleDataGenerator dataGenerator = new SimpleDataGenerator(numberOfPages);
        //NodeTree proxy = new NodeTree(root, disk.getPagedFile());

        try (PageProxyCursor cursor = disk.getCursor(9, PagedFile.PF_EXCLUSIVE_LOCK)) {
                NodeHeader.initializeLeafNode(cursor);
                cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
                cursor.putBytes(dataGenerator.nextPage());
                long i = 10000;
                long[] key = new long[]{i, i, i, i};
                while(cursor.leafNodeContainsSpaceForNewKey(key)) {
                    NodeInsertion.addKeyToLeafNode(cursor, key);
                    i++;
                    key = new long[]{i, i, i, i};
                }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Done: ");
    }
}