package bptree;

/**
 * Created by max on 3/4/15.
 */

import org.junit.Test;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.monitoring.PageCacheMonitor;

import java.io.File;
import java.io.IOException;

/**
 * Created by max on 3/2/15.
 */
public class NodeCreationTest {

    protected final File file = new File( "a" );
    protected int recordSize = 9;
    protected int maxPages = 20;
    protected int pageCachePageSize = BlockManager.PAGE_SIZE;
    protected int recordsPerFilePage = pageCachePageSize / recordSize;
    protected int recordCount = 25 * maxPages * recordsPerFilePage;
    protected int filePageSize = recordsPerFilePage * recordSize;

    protected DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    MuninnPageCache pageCache;
    PagedFile pagedFile;

    public NodeCreationTest() throws IOException {
        pageCache = new MuninnPageCache(fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL);
        pagedFile = pageCache.map(file, filePageSize);
    }
    public void manuallyWriteLBlock(int pageID) throws IOException {


        int startingPageId = 0;
        try (PageCursor cursor = pagedFile.io(pageID, PagedFile.PF_EXCLUSIVE_LOCK)) {
            if (cursor.next()) {
                do {
                    // perform read or write operations on the page
                    cursor.putByte((byte) 1); //its a leaf
                    cursor.putLong(2);//pathID
                    cursor.putLong(2); //nodeID
                    cursor.putLong(2);//nodeID
                    cursor.putLong(2);//nodeID
                    cursor.putLong(-1);
                    cursor.putLong(3);//pathID
                    cursor.putLong(3); //nodeID
                    cursor.putLong(3);//nodeID
                    cursor.putLong(3);//nodeID
                    cursor.putLong(-1);
                    cursor.putLong(4);//pathID
                    cursor.putLong(4); //nodeID
                    cursor.putLong(4);//nodeID
                    cursor.putLong(4);//nodeID
                    cursor.putLong(-1);
                    cursor.putLong(-1);
                }
                while (cursor.shouldRetry());
            }
        }
                    //now try to create the LBlock
    }

    public void manuallyWriteContigiousIBlock(int pageID) throws IOException {
        try (PageCursor cursor = pagedFile.io(pageID, PagedFile.PF_EXCLUSIVE_LOCK)) {
            if (cursor.next()) {
                do {
                    // perform read or write operations on the page
                    cursor.putByte((byte) 0); //its an internal node
                    cursor.putByte((byte) 1); //Keys are same size
                    cursor.putInt(3);//number of child pointers
                    cursor.putInt(4);//key length
                    cursor.putLong(2); //child 1
                    cursor.putLong(3);//child
                    cursor.putLong(4);//child
                    cursor.putLong(6);//pathID
                    cursor.putLong(6); //nodeID
                    cursor.putLong(6);//nodeID
                    cursor.putLong(6);//nodeID
                    cursor.putLong(7);//pathID
                    cursor.putLong(7); //nodeID
                    cursor.putLong(7);//nodeID
                    cursor.putLong(7);//nodeID
                }
                while (cursor.shouldRetry());
            }
        }
        //now try to create the LBlock
    }
    public void manuallyWriteVariableIBlock(int pageID) throws IOException {
        try (PageCursor cursor = pagedFile.io(pageID, PagedFile.PF_EXCLUSIVE_LOCK)) {
            if (cursor.next()) {
                do {
                    // perform read or write operations on the page
                    cursor.putByte((byte) 0); //its an internal node
                    cursor.putByte((byte) 0); //Keys are same size
                    cursor.putInt(3);//number of child pointers
                    cursor.putInt(4);//key length, doesnt mean anything if variable length
                    cursor.putLong(2); //child 1
                    cursor.putLong(3);//child
                    cursor.putLong(4);//child
                    cursor.putLong(6);//pathID
                    cursor.putLong(6); //nodeID
                    cursor.putLong(6);//nodeID
                    cursor.putLong(6);//nodeID
                    cursor.putLong(-1);//nodeID
                    cursor.putLong(7);//pathID
                    cursor.putLong(7); //nodeID
                    cursor.putLong(7);//nodeID
                    cursor.putLong(7);//nodeID
                    cursor.putLong(7);//nodeID
                    cursor.putLong(7);//nodeID
                    cursor.putLong(-1);//nodeID
                }
                while (cursor.shouldRetry());
            }
        }
        //now try to create the LBlock
    }

    @Test
    public void readLBlock() throws IOException {
        int pageID = 0;
        BlockManager bm = new BlockManager();
        manuallyWriteLBlock(pageID);
        try (PageCursor cursor = pagedFile.io(pageID, PagedFile.PF_EXCLUSIVE_LOCK)) {
            if (cursor.next()) {
                do {
                    LeafNode block = new LeafNode(cursor, bm, bm.getNewID());
                    System.out.println(block);
                }
                while (cursor.shouldRetry());

            }
        }

    }

    @Test
    public void readIBlock() throws IOException {
        int pageID = 1;
        BlockManager bm = new BlockManager();
        manuallyWriteVariableIBlock(pageID);
        try (PageCursor cursor = pagedFile.io(pageID, PagedFile.PF_EXCLUSIVE_LOCK)) {
            if (cursor.next()) {
                do {
                    InternalNode block = new InternalNode(cursor, bm, bm.getNewID());
                    System.out.println(block);
                    cursor.setOffset(0);
                    cursor.putBytes(block.asByteArray());
                    cursor.setOffset(0);
                    block = new InternalNode(cursor, bm, bm.getNewID());
                    System.out.println(block);
                }
                while (cursor.shouldRetry());

            }
        }

    }


}
