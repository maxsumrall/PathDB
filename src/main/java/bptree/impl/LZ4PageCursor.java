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
    byte[] compressed = new byte[DiskCache.PAGE_SIZE*6];
    byte[] decompressed = new byte[DiskCache.PAGE_SIZE * 6];
    byte[] tmp_buffer = new byte[DiskCache.PAGE_SIZE];
    LZ4Factory factory = LZ4Factory.fastestInstance();
    LZ4Compressor compressor = factory.fastCompressor();
    LZ4FastDecompressor decompressor = factory.fastDecompressor();
    ByteBuffer buffer;
    PageCursor cursor;
    int SIZE = 0;
    private boolean dirty = true;

    public LZ4PageCursor(DiskCache disk, long pageId, int lock) throws IOException {
        pagedFile = disk.getPagedFile();
        this.cursor = pagedFile.io(pageId, lock);
        this.cursor.next();
        loadDataFromCursor();
        buffer = ByteBuffer.wrap(decompressed);
    }

    private LZ4PageCursor(){

    }

    public int getSize(){
        return SIZE;
    }

    public int capacity(){
        return buffer.capacity();
        //return DiskCache.PAGE_SIZE;
    }

    public void initNewLeaf(byte[] leafData) throws IOException {
        this.decompressed = leafData;
        buffer = ByteBuffer.wrap(this.decompressed);
        compress();
    }

    private void loadDataFromCursor() throws IOException {
        cursor.setOffset(0);
        cursor.getBytes(tmp_buffer);
        decompress();
    }

    private void writeDataToCursor(){
     try {
         compress();
     } catch (IOException e) {
         e.printStackTrace();
     }
        System.arraycopy(compressed, 0, tmp_buffer, 0, SIZE);
        cursor.setOffset(0);
        cursor.putBytes(tmp_buffer);
    }

    public void compress() throws IOException {
        int maxCompressedSize = compressor.maxCompressedLength(decompressed.length);
        byte[] tmp_compressed = new byte[maxCompressedSize];
        this.SIZE = compressor.compress(decompressed, 0, decompressed.length, compressed, 0, maxCompressedSize);
        compressed = Arrays.copyOfRange(tmp_compressed,0 , this.SIZE);


        //System.out.println("Original: " + decompressed.length + " b");
        //System.out.println("Compressed: " + SIZE + " b");
    }

    public void decompress(){
        try {
            int size = decompressor.decompress(tmp_buffer, 0, decompressed, 0, decompressed.length);
            this.SIZE = size;
        }
        catch(Exception e){
            SIZE = 0;
            //System.out.println("Caught exception in decompression.");
        }
        //System.out.println("Original: " + data.length);
        //System.out.println("Compressed: " + output.length);
    }

    @Override
    public void next(long page) throws IOException {
        writeDataToCursor();
        Arrays.fill(this.decompressed, (byte) 0);
        this.buffer.position(0);
        cursor.next(page);
        loadDataFromCursor();
    }

    @Override
    public long getCurrentPageId() {
        return this.cursor.getCurrentPageId();
    }

    @Override
    public void setOffset(int offset) {
        this.buffer.position(offset);
    }

    @Override
    public int getOffset() {
        return this.buffer.position();
    }

    @Override
    public void getBytes(byte[] dest) {
        this.buffer.get(dest);
    }

    @Override
    public byte getByte(int offset) {
        return this.buffer.get(offset);
    }

    @Override
    public void putBytes(byte[] src) {
        this.buffer.put(src);
        writeDataToCursor();
    }

    @Override
    public void putByte(byte val) {
        this.buffer.put(val);
        writeDataToCursor();
    }

    @Override
    public void putByte(int offset, byte val) {
        this.buffer.put(offset, val);
        writeDataToCursor();
    }

    @Override
    public long getLong() {
        return this.buffer.getLong();
    }

    @Override
    public long getLong(int offset) {
        return this.buffer.getLong(offset);
    }

    @Override
    public void putLong(long val) {
        this.buffer.putLong(val);
        writeDataToCursor();
    }

    @Override
    public void putLong(int offset, long val) {
        this.buffer.putLong(offset, val);
        writeDataToCursor();
    }

    @Override
    public int getInt() {
        return this.buffer.getInt();
    }

    @Override
    public int getInt(int offset) {
        return this.buffer.getInt(offset);
    }

    @Override
    public void putInt(int val) {
        this.buffer.putInt(val);
        writeDataToCursor();

    }

    @Override
    public void putInt(int offset, int val) {
        this.buffer.putInt(offset, val);
        writeDataToCursor();
    }

    @Override
    public boolean internalNodeContainsSpaceForNewKeyAndChild(long[] newKey) throws IOException {
        return (this.SIZE + (newKey.length + 1) * 8) <= DiskCache.PAGE_SIZE - 50;
    }

    @Override
    public boolean leafNodeContainsSpaceForNewKey(long[] newKey) throws IOException {
        return (this.SIZE + newKey.length * 8 ) <= DiskCache.PAGE_SIZE - 50;
    }

    @Override
    public void deferWriting() {

    }

    @Override
    public void resumeWriting() {

    }

    @Override
    public void close() throws IOException {
        this.cursor.close();
    }
}
