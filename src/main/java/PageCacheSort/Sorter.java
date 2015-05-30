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
    int FAN_IN = 500;
    PageProxyCursor setIteratorCursor;
    DiskCache writeToDisk;
    DiskCache readFromDisk;
    PageProxyCursor writeToCursor;
    KeyImpl comparator = KeyImpl.getComparator();
    PageSet postSortSet;
    public static final int ALT_MAX_PAGE_SIZE = DiskCache.PAGE_SIZE - NodeHeader.NODE_HEADER_LENGTH;
    long finalPage;
    int countA = 0; //debugging
    int countB = 0; //debugging
    final int keySize;
    final int keyByteSize;
    LinkedList<PageSet> writePageSets = new LinkedList<>();
    LinkedList<PageSet> readPageSets = new LinkedList<>();
    int byteRepSize = 0;
    PriorityQueue<Long[]> bulkLoadedKeys = new PriorityQueue<>(KeyImpl.getComparator());


    public Sorter(int keySize) throws IOException {
        this.keySize = keySize;
        this.keyByteSize = this.keySize * 8;
        writeToDisk = DiskCache.getDiskCacheWithFilename("tmp_sortFileA.dat");
        readFromDisk = DiskCache.getDiskCacheWithFilename("tmp_sortFileB.dat");
        writeToCursor = writeToDisk.getCursor(0, PagedFile.PF_EXCLUSIVE_LOCK);
        //writePageSets.push(new PageSet(0));
    }

    public SetIterator sort() throws IOException {
        flushBulkLoadedKeys(); //check the contents of last page
        System.out.println("Post-Bulk, countA: " + countA + " countB: " + countB);
        System.out.println("Final Page ID: " + writeToCursor.getCurrentPageId());
        countA = 0;
        countB = 0;
        writeToCursor.close();
        //sortEachPage();
       // debug_countKeysType();
        //long startTime = System.nanoTime();
        sortHelper();
        //System.out.println("Merge Duration: " + ((System.nanoTime() - startTime) / 1000000));
        setIteratorCursor = null;
        postSortSet = writePageSets.pop();
        finalPage = postSortSet.pagesInSet.getLast();
        return getFinalIterator(writeToDisk);
    }

    private void sortHelper() throws IOException {
        swapPageSets();
        setIteratorCursor = readFromDisk.getCursor(0, PagedFile.PF_EXCLUSIVE_LOCK);
        writeToCursor = writeToDisk.getCursor(0, PagedFile.PF_EXCLUSIVE_LOCK);
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
        System.out.println("countA: " + countA + " countB: " + countB);
        System.out.println("Final Page ID: " + writeToCursor.getCurrentPageId());
        countA = 0;
        countB = 0;
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
        long[] prev = new long[]{0,0,0,0};
        long[] next;
        SetIterator curr;
        while(pQueue.size() > 0){
            curr = pQueue.poll();
            next = curr.getNext();
            assert(KeyImpl.getComparator().compare(next, prev) > 0);
            prev = next;
            addSortedKey(next);
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
        SetIterator itr = new SetIteratorImpl(postSortSet);
        return itr;
    }

    private void debug_countKeysType() throws IOException {
        HashMap<Long, Long> map = new HashMap<>();
        try(PageProxyCursor cursor = writeToDisk.getCursor(0, PagedFile.PF_EXCLUSIVE_LOCK)){
            for(PageSet set : writePageSets) {
                for (long pageId : set.getAll()) {
                    cursor.next(pageId);
                    long[] next = new long[4];
                    next[0] = cursor.getLong();
                    next[1] = cursor.getLong();
                    next[2] = cursor.getLong();
                    next[3] = cursor.getLong();
                    while (cursor.getOffset() + (4 * 8) < DiskCache.PAGE_SIZE) {
                        for (int i = 0; i < 4; i++) {
                            next[i] = cursor.getLong();
                        }
                        if (!map.containsKey(next[0])) {
                            map.put(next[0], 0l);
                        }
                        map.put(next[0], map.get(next[0]) + 1);

                    }
                }
            }
        }
        for(Long key : map.keySet()){
            System.out.println("Debug - Key: " + key + "Found vals: " + map.get(key));
        }
    }

    public void addUnsortedKey(long[] key) throws IOException {
        if(byteRepSize + (keySize * 8) > ALT_MAX_PAGE_SIZE){
            flushBulkLoadedKeys();
        }
        byteRepSize += key.length * 8;
        Long[] keyObj = new Long[key.length];
        for(int i = 0; i < key.length; i++){
            keyObj[i] = key[i];
        }
        bulkLoadedKeys.add(keyObj); //TODO optimize this crap, this primitive -> obj conversion is retarded.
    }
    public void addSortedKey(long[] key) throws IOException {
        if(byteRepSize + (keySize * 8) > ALT_MAX_PAGE_SIZE){
            flushAfterSortedKey();
        }
        byteRepSize += key.length * 8;
        Long[] keyObj = new Long[key.length];
        for(int i = 0; i < key.length; i++){
            keyObj[i] = key[i];
        }
        bulkLoadedKeys.add(keyObj); //TODO optimize this crap, this primitive -> obj conversion is retarded.
    }

    private void flushBulkLoadedKeys() throws IOException {
        //dump sorted keys to page,
        writeToCursor.putInt(byteRepSize);
        while(bulkLoadedKeys.size() > 0){
            Long[] sortedKey = bulkLoadedKeys.poll();
            for (Long val : sortedKey) {
                writeToCursor.putLong(val);
            }
            if(!sortedKey[0].equals(new Long(90603815l))){
                countA++;
            }
            else{
                countB++;
            }
        }
        bulkLoadedKeys.clear();
        byteRepSize = 0;
        writePageSets.add(new PageSet(writeToCursor.getCurrentPageId()));
        writeToCursor.next(writeToCursor.getCurrentPageId() + 1);
    }

    private void flushAfterSortedKey() throws IOException {
        writeToCursor.setOffset(0);
        writeToCursor.putInt(byteRepSize);
        //dump sorted keys to page,
        while(bulkLoadedKeys.size() > 0){
            Long[] sortedKey = bulkLoadedKeys.poll();
            for (Long val : sortedKey) {
                writeToCursor.putLong(val);
            }
            if(!sortedKey[0].equals(new Long(90603815l))){
                countA++;
            }
            else{
                countB++;
            }
        }
        writeToCursor.next(writeToCursor.getCurrentPageId() + 1);
        bulkLoadedKeys.clear();
        byteRepSize = 0;
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
                int byteAmount = setIteratorCursor.getInt();
                if(byteAmount != byteRep.length){
                    byteRep = new byte[byteAmount];
                }
                setIteratorCursor.getBytes(byteRep);
                buffer = ByteBuffer.wrap(byteRep).asLongBuffer();
            }
            else{
                try(PageProxyCursor cursor = readFromDisk.getCursor(pageId, PagedFile.PF_EXCLUSIVE_LOCK)){
                    int byteAmount = cursor.getInt();
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
            /*long[] ret = new long[keySize];
            System.arraycopy(next, 0, ret, 0, keySize);
            readKey();
            return ret;
            */

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
            buffer.position(buffer.position() - keySize);
            return ret;
        }

        public boolean hasNext() throws IOException {
            if((buffer.position()) == buffer.capacity() && set.isEmpty()) {
                return false;
            }
            if((buffer.position()) == buffer.capacity() && !set.isEmpty()){
                fillBuffer(set.pop());
            }
            //return !nextKeyAllZeros();
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
