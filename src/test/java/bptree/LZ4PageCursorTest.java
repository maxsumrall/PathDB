package bptree;

import bptree.impl.*;
import org.junit.Test;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;

/**
 * Created by max on 5/11/15.
 */
public class LZ4PageCursorTest {

    @Test
    public void loadTest() throws IOException {

        DiskCache disk = DiskCache.temporaryDiskCache(false);
        int numberOfPages = 2000; //100000 pages should roughly equal 20mil keys;

        try (PageProxyCursor cursor = disk.getCursor(9, PagedFile.PF_EXCLUSIVE_LOCK)) {
            //NodeHeader.initializeLeafNode(cursor);
            // cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
            for(long i = 0; i < numberOfPages; i++) {
                cursor.putLong(i);
                if(!cursor.leafNodeContainsSpaceForNewKey(new long[]{i+1})){
                    break;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Done: ");

    }
}