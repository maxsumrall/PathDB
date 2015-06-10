package NeoIntegration;

import bptree.PageProxyCursor;
import bptree.impl.DiskCache;
import bptree.impl.NodeBulkLoader;
import bptree.impl.NodeHeader;
import bptree.impl.NodeTree;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;

/**
 * Takes a normal index, compresses the leaves, builds the new index on it, and saves it.
 */
public class IndexCompressor {
    public DiskCache uncompressedDisk;
    public DiskCache compressedDisk;

    public static void main(String[] args) throws IOException {
        int keyLength = 4;
        IndexCompressor ic = new IndexCompressor(keyLength);
        long finalPageID = ic.compressDisk(keyLength);
        ic.buildIndex(finalPageID, keyLength);
    }

    public IndexCompressor(int keyLength) {
        String diskPath = "K4Cleverlubm50Index.db";
        this.uncompressedDisk = DiskCache.persistentDiskCache(diskPath, false);
        this.compressedDisk = DiskCache.persistentDiskCache(keyLength + "compressed_disk.db", false); //I'm handling compression here, so I don't want the cursor to get confused.
    }

    public void buildIndex(long finalPageID, int keyLength) throws IOException {
        NodeBulkLoader bulkLoader = new NodeBulkLoader(compressedDisk, finalPageID, keyLength);
        NodeTree index = bulkLoader.run();
        System.out.println("Compressed index root: " + index.rootNodeId);
    }

    public long compressDisk(int keyLength) throws IOException {
        long finalPageID;
        long[] next = new long[keyLength];
        long[] prev = new long[keyLength];
        byte[] encodedKey;
        int keyCount = 0;
        long nextPage = 0;
        try (PageProxyCursor compressedCursor = compressedDisk.getCursor(0, PagedFile.PF_EXCLUSIVE_LOCK)) {
            try (PageProxyCursor uncompressedCursor = uncompressedDisk.getCursor(nextPage, PagedFile.PF_SHARED_LOCK)) {
                compressedCursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
                while (nextPage != -1) {
                    uncompressedCursor.next(nextPage);
                    if (!NodeHeader.isLeafNode(uncompressedCursor))
                        break;
                    nextPage = NodeHeader.getSiblingID(uncompressedCursor);
                    uncompressedCursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
                    for (int i = 0; i < NodeHeader.getNumberOfKeys(uncompressedCursor); i++) {
                        for (int j = 0; j < keyLength; j++)
                            next[j] = uncompressedCursor.getLong();
                        encodedKey = encodeKey(next, prev);
                        System.arraycopy(next, 0, prev, 0, prev.length);
                        keyCount++;
                        compressedCursor.putBytes(encodedKey);
                        if ((compressedCursor.getOffset() + (keyLength * Long.BYTES)) > DiskCache.PAGE_SIZE) {
                            //finalize this buffer, write it to the page, and start a new buffer
                            NodeHeader.setFollowingID(compressedCursor, compressedCursor.getCurrentPageId() + 1);
                            NodeHeader.setPrecedingId(compressedCursor, compressedCursor.getCurrentPageId() - 1);
                            NodeHeader.setKeyLength(compressedCursor, keyLength);
                            NodeHeader.setNumberOfKeys(compressedCursor, keyCount);
                            NodeHeader.setNodeTypeLeaf(compressedCursor);
                            compressedCursor.next(compressedCursor.getCurrentPageId() + 1);
                            compressedCursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
                            keyCount = 0;
                            prev = new long[keyLength];
                        }
                    }
                }
                NodeHeader.setFollowingID(compressedCursor, -1);//last leaf node
                NodeHeader.setPrecedingId(compressedCursor, compressedCursor.getCurrentPageId() - 1);
                NodeHeader.setKeyLength(compressedCursor, keyLength);
                NodeHeader.setNumberOfKeys(compressedCursor, keyCount);
                NodeHeader.setNodeTypeLeaf(compressedCursor);
                finalPageID = compressedCursor.getCurrentPageId();
            }
        }
        compressedDisk.COMPRESSION = true;
        return finalPageID;
    }




    public static byte[] encodeKey(long[] key, long[] prev){

        long[] diff = new long[key.length];
        for(int i = 0; i < key.length; i++)
        {
            diff[i] = key[i] - prev[i];
        }

        int maxNumBytes = Math.max(numberOfBytes(diff[0]), 1);
        for(int i = 1; i < key.length; i++){
            maxNumBytes = Math.max(maxNumBytes, numberOfBytes(diff[i]));
        }

        byte[] encoded = new byte[1 + (maxNumBytes * key.length )];
        encoded[0] = (byte)maxNumBytes;
        for(int i = 0; i < key.length; i++){
            toBytes(diff[i], encoded, 1 + (i * maxNumBytes), maxNumBytes);
        }
        return encoded;
    }

    public static int numberOfBytes(long value){
        return (int) (Math.ceil(Math.log(value) / Math.log(2)) / 8) + 1;
    }

    public static void toBytes(long val, byte[] dest, int position, int numberOfBytes) { //rewrite this to put bytes in a already made array at the right position.
        for (int i = numberOfBytes - 1; i > 0; i--) {
            dest[position + i] = (byte) val;
            val >>>= 8;
        }
        dest[position] = (byte) val;
    }
}
