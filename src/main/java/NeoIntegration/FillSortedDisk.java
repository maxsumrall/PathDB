package NeoIntegration;

import bptree.PageProxyCursor;
import bptree.impl.DiskCache;
import bptree.impl.NodeHeader;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;

/**
 * Takes a normal index, compresses the leaves, builds the new index on it, and saves it.
 */
public class FillSortedDisk {
    public DiskCache compressedDisk;
    long finalPageID;
    long[] prev;
    byte[] encodedKey;
    int keyCount = 0;
    int keyLength;
    PageProxyCursor compressedCursor;

    public FillSortedDisk(int keyLength) throws IOException {
        this.keyLength = keyLength;
        this.prev = new long[keyLength];
        this.compressedDisk = DiskCache.persistentDiskCache(keyLength + "compressed_disk.db", false); //I'm handling compression here, so I don't want the cursor to get confused.
        compressedCursor = compressedDisk.getCursor(0, PagedFile.PF_EXCLUSIVE_LOCK);
        NodeHeader.setFollowingID(compressedCursor, compressedCursor.getCurrentPageId() + 1);
        NodeHeader.setPrecedingId(compressedCursor, compressedCursor.getCurrentPageId() - 1);
        NodeHeader.setKeyLength(compressedCursor, keyLength);
        NodeHeader.setNodeTypeLeaf(compressedCursor);
        compressedCursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
    }

    public void addKey(long[] next) throws IOException {
        encodedKey = encodeKey(next, prev);
        System.arraycopy(next, 0, prev, 0, prev.length);
        keyCount++;
        compressedCursor.putBytes(encodedKey);
        NodeHeader.setNumberOfKeys(compressedCursor, keyCount);
        if ((compressedCursor.getOffset() + (keyLength * Long.BYTES)) > DiskCache.PAGE_SIZE) {
            //finalize this buffer, write it to the page, and start a new buffer
            NodeHeader.setNumberOfKeys(compressedCursor, keyCount);
            compressedCursor.next(compressedCursor.getCurrentPageId() + 1);
            NodeHeader.setFollowingID(compressedCursor, compressedCursor.getCurrentPageId() + 1);
            NodeHeader.setPrecedingId(compressedCursor, compressedCursor.getCurrentPageId() - 1);
            NodeHeader.setKeyLength(compressedCursor, keyLength);
            NodeHeader.setNodeTypeLeaf(compressedCursor);
            compressedCursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
            keyCount = 0;
            prev = new long[keyLength];
        }
    }

    public long finish() throws IOException {
        NodeHeader.setFollowingID(compressedCursor, -1);//last leaf node
        NodeHeader.setPrecedingId(compressedCursor, compressedCursor.getCurrentPageId() - 1);
        NodeHeader.setKeyLength(compressedCursor, keyLength);
        NodeHeader.setNumberOfKeys(compressedCursor, keyCount);
        NodeHeader.setNodeTypeLeaf(compressedCursor);
        finalPageID = compressedCursor.getCurrentPageId();
        compressedDisk.COMPRESSION = true;
        compressedCursor.close();
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
        long abs = Math.abs(value);
        int minBytes = 8;
        if(abs <= 127){
            minBytes = 1;
        }
        else if(abs <= 32768){
            minBytes = 2;
        }
        else if(abs <= 8388608){
            minBytes = 3;
        }
        else if(abs <= 2147483648l){
            minBytes = 4;
        }
        else if(abs <= 549755813888l){
            minBytes = 5;
        }
        else if(abs <= 140737488355328l){
            minBytes = 6;
        }
        else if(abs <= 36028797018963968l){
            minBytes = 7;
        }
        return minBytes;
    }
    public static void toBytes(long val, byte[] dest, int position, int numberOfBytes) { //rewrite this to put bytes in a already made array at the right position.
        for (int i = numberOfBytes - 1; i > 0; i--) {
            dest[position + i] = (byte) val;
            val >>= 8;
        }
        dest[position] = (byte) val;
    }
}
