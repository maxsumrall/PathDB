package bptree.impl;

import bptree.PageProxyCursor;
import org.neo4j.io.pagecache.PageCursor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;


public class CompressedPageCursor extends PageProxyCursor{
    PageCursor cursor;
    int maxPageSize = DiskCache.PAGE_SIZE * 3;
    ByteBuffer dBuffer = ByteBuffer.allocate(maxPageSize);
    int mostRecentCompressedLeafSize = DiskCache.PAGE_SIZE;//the default value
    boolean deferWriting = false;

    public CompressedPageCursor(DiskCache disk, long pageId, int lock) throws IOException {
        this.cursor = disk.pagedFile.io(pageId, lock);
        cursor.next();
        loadCursorFromDisk();
    }

    @Override
    public void next(long page) throws IOException {
        cursor.next(page);
        loadCursorFromDisk();
    }

    public void pushChangesToDisk(){
        if(!deferWriting)
            if(NodeHeader.isLeafNode(cursor))
                compressAndWriteLeaf();
            else
                writeInternal();
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
        int decompressedSize = getLastUsedLeafBufferPosition() - NodeHeader.NODE_HEADER_LENGTH;
        if(decompressedSize != 0) {
            byte[] compresedMinusHeader = compress(dBuffer, NodeHeader.NODE_HEADER_LENGTH, decompressedSize);
            writeHeaderToCursor();
            cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
            cursor.putBytes(compresedMinusHeader);
        }
    }

    public byte[] compress(ByteBuffer dKeys, int offset, int length){
        int keyLength = NodeHeader.getKeyLength(cursor);
        int maxCompressedSize = length - offset;
        byte[] compressed = new byte[maxCompressedSize];
        ByteBuffer buffer = ByteBuffer.wrap(compressed);
        long[][] keys = new long[(length - offset) / keyLength][keyLength];
        dKeys.position(NodeHeader.NODE_HEADER_LENGTH);
        for(int i = 0; i < keys.length; i++)
            for(int j = 0; j < keyLength; j++)
                keys[i][j] = dKeys.getLong();
        buffer.put(encodeKey(keys[0], new long[keys[0].length]));
        for(int i = 1; i < keys.length; i++)
            buffer.put(encodeKey(keys[i], keys[i-1]));
        byte[] truncatedCompressed = new byte[buffer.position()];
        System.arraycopy(compressed, 0, truncatedCompressed, 0, truncatedCompressed.length);
        return truncatedCompressed;
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

        byte[] encoded = new byte[1 + (maxNumBytes * 3 )];
        encoded[0] = (byte)maxNumBytes;
        for(int i = 0; i < key.length; i++){
            toBytes(diff[i], encoded, 1 + (i * maxNumBytes), maxNumBytes);
        }
        return encoded;
    }
    public static int numberOfBytes(long value){
        return (int) (Math.ceil(Math.log(value) / Math.log(2)) / 8) + 1;
    }

    public static void toBytes(long val, byte[] dest,int position, int numberOfBytes) { //rewrite this to put bytes in a already made array at the right position.
        for (int i = numberOfBytes - 1; i > 0; i--) {
            dest[position + i] = (byte) val;
            val >>>= 8;
        }
        dest[position] = (byte) val;
    }
    public static byte[] toBytes(long val) {
        int numberOfBytes = (int) (Math.ceil(Math.log(val) / Math.log(2)) / 8) + 1;
        byte [] b = new byte[numberOfBytes];
        for (int i = numberOfBytes - 1; i > 0; i--) {
            b[i] = (byte) val;
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
    }

    public static long toLong(byte[] bytes) {
        long l = 0;
        for(int i = 0; i < bytes.length; i++) {
            l <<= 8;
            l ^= bytes[i] & 0xFF;
        }
        return l;
    }

    public static long toLong(byte[] bytes, int offset, int length) {
        long l = 0;
        for(int i = offset; i < offset + length; i++) {
            l <<= 8;
            l ^= bytes[i] & 0xFF;
        }
        return l;
    }
    public static long toLong(PageCursor cursor, int offset, int length) {
        long l = 0;
        for(int i = offset; i < offset + length; i++) {
            l <<= 8;
            l ^= cursor.getByte(i) & 0xFF;
        }
        return l;
    }


    private int getLastUsedLeafBufferPosition(){
        int keyLength = NodeHeader.getKeyLength(cursor);
        int numberOfKeys = NodeHeader.getNumberOfKeys(cursor);
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
        if(NodeHeader.isUninitializedNode(cursor)){
            return;
        }
        else if(NodeHeader.isLeafNode(cursor))
            decompressLeaf();
        else
            decompressInternalNode();

    }

    private void decompressLeaf(){
        int keyLength = NodeHeader.getKeyLength(cursor);
        int numberOfKeys = NodeHeader.getNumberOfKeys(cursor);
        dBuffer.limit(maxPageSize);
        Arrays.fill(dBuffer.array(), (byte)0);
        dBuffer.position(0);
        cursor.setOffset(0);
        for(int i = 0; i < NodeHeader.NODE_HEADER_LENGTH; i++){
            dBuffer.put(cursor.getByte());
        }

        int position = NodeHeader.NODE_HEADER_LENGTH;
        int reqBytes = cursor.getByte(position);
        position += 1 + (reqBytes * keyLength);
        long val;
        long[] prev = new long[keyLength];
        for(int i = 0; i < keyLength; i++) {
            val = toLong(cursor, i + 1, reqBytes);
            prev[i] = val;
            dBuffer.putLong(val);
        }
        for(int i = 1; i < numberOfKeys; i++){
            reqBytes = cursor.getByte(position++);
            for(int j = 0; j < keyLength; j++){
                val = prev[j] + toLong(cursor, position, reqBytes);
                prev[j] = val;
                position += reqBytes;
            }
        }
        mostRecentCompressedLeafSize = position;
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
        return dBuffer.limit();
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
        dBuffer.put(val);
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
        //return NodeSize.leafNodeContainsSpaceForNewKey(this, newKey);
        return mostRecentCompressedLeafSize + (newKey.length * Long.BYTES) < DiskCache.PAGE_SIZE;
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
