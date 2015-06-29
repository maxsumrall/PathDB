package PageCacheSort;

import bptree.PageProxyCursor;
import bptree.impl.DiskCache;
import bptree.impl.KeyImpl;
import bptree.impl.NodeHeader;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.*;

/**
 * Main entry into the Sorter.
 */
public class Sorter {
    public int FAN_IN = 64;
    PageProxyCursor setIteratorCursor;
    DiskCache writeToDisk;
    DiskCache readFromDisk;
    PageProxyCursor writeToCursor;
    KeyImpl comparator = KeyImpl.getComparator();
    PageSet postSortSet;
    public static final int ALT_MAX_PAGE_SIZE = DiskCache.PAGE_SIZE - NodeHeader.NODE_HEADER_LENGTH;
    long finalPage;
    public final int keySize;
    final int keyByteSize;
    LinkedList<PageSet> writePageSets = new LinkedList<>();
    LinkedList<PageSet> readPageSets = new LinkedList<>();
    int byteRepSize = 0;
    PriorityQueue<long[]> bulkLoadedKeys = new PriorityQueue<>(KeyImpl.getComparator());
    ArrayList<long[]> sortedKeys = new ArrayList<>();

    public Sorter(int keySize) throws IOException {
        this.keySize = keySize;
        this.keyByteSize = this.keySize * 8;
        writeToDisk = DiskCache.persistentDiskCache(keySize+"tmp_sortFileA.dat", false);
        readFromDisk = DiskCache.temporaryDiskCache(keySize+"tmp_sortFileB.dat", false);
        writeToCursor = writeToDisk.getCursor(0, PagedFile.PF_EXCLUSIVE_LOCK | PagedFile.PF_NO_FAULT);
    }

    public SetIterator finishWithoutSort() throws IOException {
        flushAfterSortedKey();
        writeToCursor.next(writeToCursor.getCurrentPageId() - 1);
        NodeHeader.setFollowingID(writeToCursor, -1);
        writeToCursor.close();
        readFromDisk.shutdown();
        readFromDisk.pageCacheFile.delete();
        setIteratorCursor = null;
        postSortSet = new PageSet();
        for(PageSet pageSet : writePageSets){
            postSortSet.add(pageSet.pop());
        }
        finalPage = postSortSet.pagesInSet.getLast();
        return getFinalIterator(writeToDisk);
    }

    public SetIterator sort() throws IOException {
        flushBulkLoadedKeys(); //check the contents of last page
        writeToCursor.next(writeToCursor.getCurrentPageId() - 1);
        NodeHeader.setFollowingID(writeToCursor, -1);
        writeToCursor.close();

        sortHelper();
        readFromDisk.shutdown();
        readFromDisk.pageCacheFile.delete();
        setIteratorCursor = null;
        postSortSet = writePageSets.pop();
        finalPage = postSortSet.pagesInSet.getLast();
        return getFinalIterator(writeToDisk);
    }

    private void sortHelper() throws IOException {
        swapPageSets();
        setIteratorCursor = readFromDisk.getCursor(0, PagedFile.PF_SHARED_LOCK | PagedFile.PF_READ_AHEAD);
        writeToCursor = writeToDisk.getCursor(0, PagedFile.PF_EXCLUSIVE_LOCK | PagedFile.PF_NO_FAULT);
        while(!readPageSets.isEmpty()){
            int modifiedFanOut = Math.min(readPageSets.size(), FAN_IN);
            LinkedList<PageSet> pageSets = new LinkedList<>();
            for(int i = 0; i < modifiedFanOut; i++){
                PageSet nextSet = readPageSets.pop();
                pageSets.add(nextSet);
            }
            if(readPageSets.size() == 1){
                PageSet nextSet = readPageSets.pop();
                pageSets.add(nextSet);
            }
            mergeSets(pageSets);
        }
        flushAfterSortedKey();
        setIteratorCursor.close();
        writeToCursor.close();
        if(writePageSets.size() > 1){
            sortHelper();
        }
    }

    private void mergeSets(LinkedList<PageSet> pageSets) throws IOException {
        writePageSets.add(new PageSet(pageSets));
        PriorityQueue<SetIterator> pQueue = new PriorityQueue<>();
        for(PageSet set : pageSets){
            pQueue.add(new SetIteratorImpl(set));
        }
        SetIterator curr;
        while(pQueue.size() > 0){
            curr = pQueue.poll();
            addSortedKey(curr.getNext());
            if(curr.hasNext()) {
                pQueue.add(curr);
            }
        }
    }

    private void swapPageSets() throws IOException {
        DiskCache tmpDisk = writeToDisk;
        writeToDisk = readFromDisk;
        readFromDisk = tmpDisk;

        LinkedList<PageSet> tmp = this.writePageSets;
        this.writePageSets = this.readPageSets;
        this.readPageSets = tmp;
    }

    public DiskCache getSortedDisk(){
        return writeToDisk;
    }
    public long finalPageId(){
        return finalPage;
    }

    public void print(DiskCache sortedNumbersDisk) throws IOException {
        readFromDisk = sortedNumbersDisk;

        SetIterator itr = new SetIteratorImpl(postSortSet);
        while(itr.hasNext()){
            System.out.println(Arrays.toString(itr.getNext()));
        }
    }
    private SetIterator getFinalIterator(DiskCache sortedNumbersDisk) throws IOException {
        readFromDisk = sortedNumbersDisk;
        return new SetIteratorImpl(postSortSet);
    }

    public void addUnsortedKey(long[] key) throws IOException {
        assert(key.length == keySize);
        if(byteRepSize + (keySize * 8) >= ALT_MAX_PAGE_SIZE){
            flushBulkLoadedKeys();
        }
        byteRepSize += key.length * 8;
        bulkLoadedKeys.add(key);
    }
    private void addSortedKey(long[] key) throws IOException {
        if(byteRepSize + (keySize * 8) >= ALT_MAX_PAGE_SIZE){
            flushAfterSortedKey();
        }
        byteRepSize += key.length * 8;
        sortedKeys.add(key);
    }

    /* Testing for compressed pages.
    public void addSortedKeyBulk(Long[] key) throws IOException {
        if(byteRepSize + (keySize * 8) >= ALT_MAX_PAGE_SIZE){
            flushSortedBulkLoadedKeys();
        }
        byteRepSize += key.length * 8;
        sortedKeys.add(key);
    }
    */

    public void addSortedKeyBulk(long[] key) throws IOException {
        if(byteRepSize + (keySize * 8) >= ALT_MAX_PAGE_SIZE){
            flushSortedBulkLoadedKeys();
        }
        byteRepSize += key.length * 8;
        sortedKeys.add(key);
    }


    private void flushSortedBulkLoadedKeys() throws IOException {
        //dump sorted keys to page,
        NodeHeader.setNodeTypeLeaf(writeToCursor);
        NodeHeader.setKeyLength(writeToCursor, keySize);
        NodeHeader.setNumberOfKeys(writeToCursor, sortedKeys.size());
        NodeHeader.setPrecedingId(writeToCursor, writeToCursor.getCurrentPageId() - 1);
        NodeHeader.setFollowingID(writeToCursor, writeToCursor.getCurrentPageId() + 1);
        writeToCursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        for(long[] sortedKey : sortedKeys){
            for (long val : sortedKey) {
                writeToCursor.putLong(val);
            }
        }
        sortedKeys.clear();
        byteRepSize = 0;
        writePageSets.add(new PageSet(writeToCursor.getCurrentPageId()));
        writeToCursor.next(writeToCursor.getCurrentPageId() + 1);
    }

    private void flushBulkLoadedKeys() throws IOException {
        //dump sorted keys to page,
        NodeHeader.setNodeTypeLeaf(writeToCursor);
        NodeHeader.setKeyLength(writeToCursor, keySize);
        NodeHeader.setNumberOfKeys(writeToCursor, bulkLoadedKeys.size());
        NodeHeader.setPrecedingId(writeToCursor, writeToCursor.getCurrentPageId() - 1);
        NodeHeader.setFollowingID(writeToCursor, writeToCursor.getCurrentPageId() + 1);
        writeToCursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        while(bulkLoadedKeys.size() > 0){
            long[] sortedKey = bulkLoadedKeys.poll();
            for (long val : sortedKey) {
                writeToCursor.putLong(val);
            }
        }
        bulkLoadedKeys.clear();
        byteRepSize = 0;
        writePageSets.add(new PageSet(writeToCursor.getCurrentPageId()));
        writeToCursor.next(writeToCursor.getCurrentPageId() + 1);
    }

    private void flushAfterSortedKey() throws IOException {
        NodeHeader.setNodeTypeLeaf(writeToCursor);
        NodeHeader.setKeyLength(writeToCursor, keySize);
        NodeHeader.setNumberOfKeys(writeToCursor, sortedKeys.size());
        NodeHeader.setPrecedingId(writeToCursor, writeToCursor.getCurrentPageId() - 1);
        NodeHeader.setFollowingID(writeToCursor, writeToCursor.getCurrentPageId() + 1);
        writeToCursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
        //dump sorted keys to page,
        for(long[] sortedKey : sortedKeys){
            for (long val : sortedKey) {
                writeToCursor.putLong(val);
            }
        }
        writeToCursor.next(writeToCursor.getCurrentPageId() + 1);
        sortedKeys.clear();
        byteRepSize = 0;
    }
    public String toString(){
        return "K" + keySize;
    }


    public class SetIteratorImpl implements SetIterator, Comparable<SetIterator>{
        boolean setExhausted = false;
        PageSet set;
        byte[] byteRep = new byte[ALT_MAX_PAGE_SIZE];
        LongBuffer buffer = ByteBuffer.wrap(byteRep).asLongBuffer();

        public SetIteratorImpl(PageSet set) throws IOException {
            this.set = set;
            fillBuffer(set.pop());
        }

        private void fillBuffer(long pageId) throws IOException {
            if(setIteratorCursor != null) {
                setIteratorCursor.next(pageId);
                int byteAmount = NodeHeader.getNumberOfKeys(setIteratorCursor) * keySize * 8;
                setIteratorCursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
                if(byteAmount != byteRep.length){
                    byteRep = new byte[byteAmount];
                }
                setIteratorCursor.getBytes(byteRep);
                buffer = ByteBuffer.wrap(byteRep).asLongBuffer();
            }
            else{
                try(PageProxyCursor cursor = readFromDisk.getCursor(pageId, PagedFile.PF_SHARED_LOCK)){
                    int byteAmount = NodeHeader.getNumberOfKeys(cursor) * keySize * 8;
                    cursor.setOffset(NodeHeader.NODE_HEADER_LENGTH);
                    if(byteAmount != byteRep.length){
                        byteRep = new byte[byteAmount];
                    }
                    cursor.getBytes(byteRep);
                    buffer = ByteBuffer.wrap(byteRep).asLongBuffer();
                }
            }
            buffer.position(0);
        }

        public long[] getNext() throws IOException {
            if(hasNext()){
                long[] next = new long[keySize];
                for (int i = 0; i < keySize; i++) {
                    next[i] = buffer.get();
                }
                return next;
            }
            return null;
        }

        public long[] peekNext() throws IOException {
            long[] ret = getNext();
            if(ret != null) {
                buffer.position(buffer.position() - keySize);
            }
            return ret;
        }

        public boolean hasNext() throws IOException {
            if((buffer.position()+keySize) >  buffer.capacity() && set.isEmpty()) {
                return false;
            }
            if((buffer.position()+keySize) > buffer.capacity() && !set.isEmpty()){
                fillBuffer(set.pop());
            }
            return true;
        }

        @Override
        public int compareTo(SetIterator other) {
            try {
                return KeyImpl.getComparator().compare(peekNext(), other.peekNext());
            } catch (IOException e) {
                return 1;
            }
        }
    }

    public class PageSet{
        public LinkedList<Long> pagesInSet;
        public PageSet(){
            pagesInSet = new LinkedList<>();
        }
        public PageSet(long pageId){
            pagesInSet = new LinkedList<>();
            pagesInSet.push(pageId);
        }
        public PageSet(PageSet a, PageSet b){
            pagesInSet = new LinkedList<>();
            pagesInSet.addAll(a.getAll());
            pagesInSet.addAll(b.getAll());
        }
        public PageSet(LinkedList<PageSet> sets){
            pagesInSet = new LinkedList<>();
            for(PageSet set : sets){
                pagesInSet.addAll(set.pagesInSet);
            }
        }
        public PageSet(PageSet a){
            pagesInSet = new LinkedList<>();
            pagesInSet.addAll(a.getAll());
        }
        public void add(long pageId) {
            pagesInSet.add(pageId);
        }
        public boolean isEmpty(){
            return pagesInSet.isEmpty();
        }
        public Long pop(){
            return pagesInSet.pop();
        }
        public Long peek() {return pagesInSet.peek();}
        public List<Long> getAll(){return pagesInSet;}
    }

}
