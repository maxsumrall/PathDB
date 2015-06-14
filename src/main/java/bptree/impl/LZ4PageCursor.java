package bptree.impl;

import bptree.PageProxyCursor;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;


/**
 * Created by max on 6/8/15.
 */
public class LZ4PageCursor extends PageProxyCursor{
    public static PagedFile pagedFile;
    LZ4Factory factory = LZ4Factory.fastestJavaInstance();
    LZ4Compressor compressor = factory.fastCompressor();
    LZ4FastDecompressor decompressor = factory.fastDecompressor();
    PageCursor cursor;
    int maxPageSize = DiskCache.PAGE_SIZE * 7;
    ByteBuffer dBuffer = ByteBuffer.allocate(maxPageSize);
    int mostRecentCompressedLeafSize = NodeHeader.NODE_HEADER_LENGTH;//the default value
    boolean deferWriting = false;

    public LZ4PageCursor(DiskCache disk, long pageId, int lock) throws IOException {
        this.cursor = disk.pagedFile.io(pageId, lock);
        cursor.next();
        loadCursorFromDisk();
    }

    @Override
    public void next(long page) throws IOException {
        forcePushChangesToDisk();
        cursor.next(page);
        mostRecentCompressedLeafSize = NodeHeader.NODE_HEADER_LENGTH;
        loadCursorFromDisk();
        dBuffer.position(0);
    }

    public void pushChangesToDisk(){
        if(dBuffer.position() >= DiskCache.PAGE_SIZE)
            forcePushChangesToDisk();
    }


    public void forcePushChangesToDisk(){
        if(!deferWriting) {
            int mark = dBuffer.position();
            if (NodeHeader.isLeafNode(dBuffer))
                compressAndWriteLeaf();
            else
                writeInternal();
            dBuffer.position(mark);
        }

    }

    private void writeInternal(){
        dBuffer.position(0);
        cursor.setOffset(0);
        for(int i = 0; i < DiskCache.PAGE_SIZE; i++){
            cursor.putByte(dBuffer.get());
        }
    }


    private void compressAndWriteLeaf(){
        //compress the contents of dBuffer. Must first determine how big dBuffer actually is.
        //assumption that this is a leaf node.
        //will just check if the path id is zero, if so, this is the end of this block.
        writeHeaderToCursor();
        int decompressedSize = getLastUsedLeafBufferPosition() - NodeHeader.NODE_HEADER_LENGTH;
        if(decompressedSize != 0) {
            byte[] compresedMinusHeader = compress(decompressedSize);
            cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
            cursor.putBytes(compresedMinusHeader);
        }
    }

    public byte[] compress(int decompressedSize){
        byte[] data = new byte[decompressedSize];
        System.arraycopy(dBuffer.array(), NodeHeader.NODE_HEADER_LENGTH, data, 0, data.length);
        byte[] compressed = new byte[compressor.maxCompressedLength(decompressedSize)];
        this.mostRecentCompressedLeafSize = compressor.compress(data, 0, decompressedSize, compressed, 0, compressed.length);
        byte[] compressedTruncated = new byte[mostRecentCompressedLeafSize];
        System.arraycopy(compressed, 0, compressedTruncated, 0, compressedTruncated.length);
        return compressedTruncated;
    }



    private int getLastUsedLeafBufferPosition(){
        int keyLength = NodeHeader.getKeyLength(cursor);
        int numberOfKeys = NodeHeader.getNumberOfKeys(dBuffer);
        if(keyLength == 0 || numberOfKeys == 0)
            return NodeHeader.NODE_HEADER_LENGTH;
        dBuffer.position(NodeHeader.NODE_HEADER_LENGTH);
        while(dBuffer.remaining() > keyLength * Long.BYTES){
            if(dBuffer.getLong() == 0l){
                dBuffer.position(dBuffer.position() - Long.BYTES);
                break;
            }
            dBuffer.position(dBuffer.position() + (keyLength -1) * Long.BYTES);
        }
        return dBuffer.position();
    }

    private void writeHeaderToCursor(){
        cursor.setOffset(0);
        for(int i = 0; i < NodeHeader.NODE_HEADER_LENGTH; i++)
            cursor.putByte(dBuffer.get(i));
    }

    private void loadCursorFromDisk(){
        //ByteBuffer possibleBuffer = fastCache.getByteBuffer(cursor.getCurrentPageId());
        //if(possibleBuffer != null){
        //    dBuffer = possibleBuffer;
        // }
        // else {
        //      dBuffer = ByteBuffer.allocate(maxPageSize);
        if (NodeHeader.isUninitializedNode(cursor)) {
            Arrays.fill(dBuffer.array(), (byte)0);
        } else if (NodeHeader.isLeafNode(cursor))
            decompressLeaf();
        else
            decompressInternalNode();
    }
    //}

    private void decompressLeaf(){
        dBuffer.position(0);
        cursor.setOffset(0);
        for(int i = 0; i < NodeHeader.NODE_HEADER_LENGTH; i++){
            dBuffer.put(cursor.getByte());
        }

        byte[] compressed = new byte[DiskCache.PAGE_SIZE - NodeHeader.NODE_HEADER_LENGTH];
        cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        cursor.getBytes(compressed);
        byte[] restored = new byte[maxPageSize - NodeHeader.NODE_HEADER_LENGTH];
        decompressor.decompress(compressed, 0, restored, 0, restored.length);
        System.arraycopy(restored, 0, dBuffer.array(), NodeHeader.NODE_HEADER_LENGTH, restored.length);
    }

    private void decompressInternalNode(){
        Arrays.fill(dBuffer.array(), (byte)0);
        dBuffer.position(0);
        cursor.setOffset(0);
        dBuffer.limit(DiskCache.PAGE_SIZE);
        for(int i = 0; i < DiskCache.PAGE_SIZE; i++){
            dBuffer.put(cursor.getByte());
        }
    }

    @Override
    public long getCurrentPageId() {
        return cursor.getCurrentPageId();
    }

    @Override
    public int capacity() {
        if(NodeHeader.isLeafNode(this))
            return maxPageSize;
        else
            return DiskCache.PAGE_SIZE;
    }

    @Override
    public int getSize(){
        return DiskCache.PAGE_SIZE;
    }

    @Override
    public void setOffset(int offset) {
        dBuffer.position(offset);
    }

    @Override
    public int getOffset() {
        return dBuffer.position();
    }

    @Override
    public void getBytes(byte[] dest) {
        dBuffer.get(dest);
    }

    @Override
    public byte getByte(int offset) {
        return dBuffer.get(offset);
    }

    @Override
    public void putBytes(byte[] src) {
        dBuffer.put(src);
        pushChangesToDisk();
    }

    @Override
    public void putByte(byte val){
        dBuffer.put(val);
        pushChangesToDisk();
    }

    @Override
    public void putByte(int offset, byte val){
        dBuffer.put(offset, val);
        pushChangesToDisk();
    }

    @Override
    public long getLong() {
        return dBuffer.getLong();
    }

    @Override
    public long getLong(int offset) {
        return dBuffer.getLong(offset);
    }

    @Override
    public void putLong(long val){
        dBuffer.putLong(val);
        pushChangesToDisk();
    }
    @Override
    public void putLong(int offset, long val){
        dBuffer.putLong(offset, val);
        pushChangesToDisk();
    }

    @Override
    public int getInt() {
        return dBuffer.getInt();
    }

    @Override
    public int getInt(int offset) {
        return dBuffer.getInt(offset);
    }

    @Override
    public void putInt(int val){
        dBuffer.putInt(val);
        pushChangesToDisk();
    }
    @Override
    public void putInt(int offset, int val){
        dBuffer.putInt(offset, val);
        pushChangesToDisk();
    }
    @Override
    public boolean leafNodeContainsSpaceForNewKey(long[] newKey){
        int magic_pad = 100;
        return (mostRecentCompressedLeafSize + (newKey.length * Long.BYTES) + magic_pad) < DiskCache.PAGE_SIZE;
    }

    @Override
    public void deferWriting() {
        deferWriting = true;
    }

    @Override
    public void resumeWriting() {
        deferWriting = false;
        pushChangesToDisk();//maybe not necessary, and this is maybe redundant.
    }

    @Override
    public boolean internalNodeContainsSpaceForNewKeyAndChild(long[] newKey){
        return NodeSize.internalNodeContainsSpaceForNewKeyAndChild(this, newKey);
    }

    @Override
    public void close() throws IOException {
        this.cursor.close();
    }
}
