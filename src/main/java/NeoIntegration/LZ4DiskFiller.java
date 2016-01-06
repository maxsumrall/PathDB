package NeoIntegration;

import bptree.PageProxyCursor;
import bptree.impl.DiskCache;
import bptree.impl.NodeHeader;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Takes a normal index, compresses the leaves, builds the new index on it, and saves it.
 */
public class LZ4DiskFiller {
    LZ4Factory factory = LZ4Factory.fastestInstance();
    LZ4Compressor compressor = factory.fastCompressor();
    public DiskCache compressedDisk;
    long finalPageID;
    long[] prev;
    int keyCount = 0;
    int keyLength;
    byte[] decompressed = new byte[DiskCache.PAGE_SIZE * 12];
    ByteBuffer buffer = ByteBuffer.wrap(decompressed);
    byte[] compressed = new byte[DiskCache.PAGE_SIZE * 12];
    ByteBuffer cBuffer = ByteBuffer.wrap(compressed);
    int maxNumBytes;
    PageProxyCursor compressedCursor;

    public LZ4DiskFiller(int keyLength) throws IOException {
        this.keyLength = keyLength;
        this.prev = new long[keyLength];
        this.compressedDisk = DiskCache.persistentDiskCache(keyLength + "BenchmarkLZ4compressed_disk.db", false); //I'm handling compression here, so I don't want the cursor to get confused.
        compressedCursor = compressedDisk.getCursor(0, PagedFile.PF_EXCLUSIVE_LOCK);
        NodeHeader.setFollowingID(compressedCursor, compressedCursor.getCurrentPageId() + 1);
        NodeHeader.setPrecedingId(compressedCursor, compressedCursor.getCurrentPageId() - 1);
        NodeHeader.setKeyLength(compressedCursor, keyLength);
        NodeHeader.setNodeTypeLeaf(compressedCursor);
        compressedCursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
    }

    public void addKey(long[] next) throws IOException {
        for(long val : next)
            buffer.putLong(val);
        int compressedLength = compressor.compress(buffer, 0, buffer.position(), cBuffer, 0, cBuffer.capacity());
        keyCount++;
        if (compressedLength + NodeHeader.NODE_HEADER_LENGTH > DiskCache.PAGE_SIZE) {
            //finalize this buffer, write it to the page, and start a new buffer
            compressedLength = compressor.compress(buffer, 0, buffer.position() - (keyLength * Long.BYTES), cBuffer, 0, cBuffer.capacity());
            for(int i = 0; i < compressedLength; i++){
                compressedCursor.putByte(cBuffer.get(i));
            }
            NodeHeader.setNumberOfKeys(compressedCursor, keyCount);
            compressedCursor.next(compressedCursor.getCurrentPageId() + 1);
            NodeHeader.setFollowingID(compressedCursor, compressedCursor.getCurrentPageId() + 1);
            NodeHeader.setPrecedingId(compressedCursor, compressedCursor.getCurrentPageId() - 1);
            NodeHeader.setKeyLength(compressedCursor, keyLength);
            NodeHeader.setNodeTypeLeaf(compressedCursor);
            compressedCursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
            keyCount = 0;
            prev = new long[keyLength];
            buffer.position(0);
            cBuffer.position(0);
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


}
