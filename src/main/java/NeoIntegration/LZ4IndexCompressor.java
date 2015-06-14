package NeoIntegration;

import bptree.PageProxyCursor;
import bptree.impl.*;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;

/**
 * Takes a normal index, compresses the leaves, builds the new index on it, and saves it.
 */
public class LZ4IndexCompressor {
    public DiskCache uncompressedDisk;
    public DiskCache compressedDisk;

    public static void main(String[] args) throws IOException {
        int keyLength = 4;
        LZ4IndexCompressor ic = new LZ4IndexCompressor(keyLength);
        long finalPageID = ic.compressDisk(keyLength);
        ic.buildIndex(finalPageID, keyLength);
    }

    public LZ4IndexCompressor(int keyLength) {
        String diskPath = "LUBM50Index/K4Cleverlubm50Index.db";
        this.uncompressedDisk = DiskCache.persistentDiskCache(diskPath, false);
        this.compressedDisk = DiskCache.temporaryDiskCache(keyLength + "compressed_disk.db", true);
    }

    public void buildIndex(long finalPageID, int keyLength) throws IOException {
        NodeBulkLoader bulkLoader = new NodeBulkLoader(compressedDisk, finalPageID, keyLength);
        NodeTree index = bulkLoader.run();
        System.out.println("Compressed index root: " + index.rootNodeId);
        compressedDisk.shutdown();
    }

    public long compressDisk(int keyLength) throws IOException {
        long finalPageID;
        long[] next = new long[keyLength];
        long[] prev = new long[keyLength];
        byte[] encodedKey;
        int keyCount = 0;
        long nextPage = 0;
        try (PageProxyCursor compressedCursor = compressedDisk.getCursor(0, PagedFile.PF_EXCLUSIVE_LOCK)) {
            compressedCursor.deferWriting();
            NodeHeader.setNodeTypeLeaf(compressedCursor);
            NodeHeader.setKeyLength(compressedCursor, keyLength);
            NodeHeader.setFollowingID(compressedCursor, compressedCursor.getCurrentPageId() + 1);
            NodeHeader.setPrecedingId(compressedCursor, compressedCursor.getCurrentPageId() - 1);
            compressedCursor.resumeWriting();
            compressedCursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
            try (PageProxyCursor uncompressedCursor = uncompressedDisk.getCursor(nextPage, PagedFile.PF_SHARED_LOCK)) {
                while (nextPage != -1) {
                    uncompressedCursor.next(nextPage);
                    if (!NodeHeader.isLeafNode(uncompressedCursor))
                        break;
                    nextPage = NodeHeader.getSiblingID(uncompressedCursor);
                    uncompressedCursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
                    for (int i = 0; i < NodeHeader.getNumberOfKeys(uncompressedCursor); i++) {
                        for (int j = 0; j < keyLength; j++)
                            next[j] = uncompressedCursor.getLong();
                        if (!compressedCursor.leafNodeContainsSpaceForNewKey(next)) {
                            //finalize this buffer, write it to the page, and start a new buffer
                            NodeHeader.setNumberOfKeys(compressedCursor, keyCount);
                            compressedCursor.next(compressedCursor.getCurrentPageId() + 1);
                            compressedCursor.deferWriting();
                            NodeHeader.setNodeTypeLeaf(compressedCursor);
                            NodeHeader.setKeyLength(compressedCursor, keyLength);
                            NodeHeader.setFollowingID(compressedCursor, compressedCursor.getCurrentPageId() + 1);
                            NodeHeader.setPrecedingId(compressedCursor, compressedCursor.getCurrentPageId() - 1);
                            compressedCursor.resumeWriting();
                            compressedCursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
                            keyCount = 0;
                        }
                        compressedCursor.deferWriting();
                        for(long val : next){
                            compressedCursor.putLong(val);
                        }
                        NodeHeader.setNumberOfKeys(compressedCursor, keyCount++);
                        compressedCursor.resumeWriting();
                        //keyCount++;
                    }
                }
                compressedCursor.deferWriting();
                NodeHeader.setFollowingID(compressedCursor, -1);//last leaf node
                NodeHeader.setPrecedingId(compressedCursor, compressedCursor.getCurrentPageId() - 1);
                NodeHeader.setKeyLength(compressedCursor, keyLength);
                NodeHeader.setNumberOfKeys(compressedCursor, keyCount);
                NodeHeader.setNodeTypeLeaf(compressedCursor);
                compressedCursor.resumeWriting();
                finalPageID = compressedCursor.getCurrentPageId();
            }
        }
        return finalPageID;
        //
    }
}
