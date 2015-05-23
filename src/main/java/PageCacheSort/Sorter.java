package PageCacheSort;

import bptree.PageProxyCursor;
import bptree.impl.DiskCache;
import bptree.impl.KeyImpl;
import org.neo4j.io.pagecache.PagedFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.*;

/**
 * Main entry into the Sorter.
 */
public class Sorter {
    PageProxyCursor setIteratorCursor;
    DiskCache writeToDisk;
    DiskCache readFromDisk;
    PageProxyCursor writeToCursor;
    KeyImpl comparator = KeyImpl.getComparator();
    PageSet postSortSet;
    long finalPage;
    final int keySize;
    final int keyByteSize;
    LinkedList<PageSet> writePageSets = new LinkedList<>();
    LinkedList<PageSet> readPageSets = new LinkedList<>();


    public Sorter(int keySize) throws IOException {
        this.keySize = keySize;
        this.keyByteSize = this.keySize * 8;
        writeToDisk = DiskCache.getDiskCacheWithFilename("tmp_sortFileA.dat");
        readFromDisk = DiskCache.getDiskCacheWithFilename("tmp_sortFileB.dat");
        writeToCursor = writeToDisk.getCursor(0, PagedFile.PF_EXCLUSIVE_LOCK);
        writePageSets.push(new PageSet(0));
    }

    public SetIterator sort() throws IOException {
        writeToCursor.close();
        sortEachPage();
        sortHelper();
        setIteratorCursor = null;
        postSortSet = writePageSets.pop();
        finalPage = postSortSet.pagesInSet.getLast();
        return getFinalIterator(writeToDisk);
    }

    public DiskCache getSortedDisk(){
        return writeToDisk;
    }
    public long finalPageId(){
        return finalPage;
    }

    private void sortHelper() throws IOException {
        swapPageSets();
        PageSet setA;
        PageSet setB;
        setIteratorCursor = readFromDisk.getCursor(0, PagedFile.PF_SHARED_LOCK);
        writeToCursor = writeToDisk.getCursor(0, PagedFile.PF_EXCLUSIVE_LOCK);
        while(!readPageSets.isEmpty()){
            //get two read cursors, read and sort, push sets to other stack

            if(readPageSets.size() == 1){
                //write single entity down
                appendOddSet(readPageSets.pop());
            }
            else{
                setA = readPageSets.pop();
                setB = readPageSets.pop();
                mergeTwoSets(setA, setB);

            }
        }
        setIteratorCursor.close();
        writeToCursor.close();
        if(writePageSets.size() > 1){
            sortHelper();
        }
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

    private void mergeTwoSets(PageSet setA, PageSet setB) throws IOException {
        writePageSets.add(new PageSet(setA, setB));
        SetIterator itrA = new SetIteratorImpl(setA);
        SetIterator itrB = new SetIteratorImpl(setB);
        long[] prev = new long[]{0,0,0,0};
        long[] next;
        while(itrA.hasNext() && itrB.hasNext()){
            if(comparator.compare(itrA.peekNext(), itrB.peekNext()) >= 0){
                addSortedKey(itrB.getNext());
            }
            else{
                addSortedKey(itrA.getNext());
            }
        }
        while(itrA.hasNext()){
            addSortedKey(itrA.getNext());
        }
        while(itrB.hasNext()){
            addSortedKey(itrB.getNext());
        }
    }
    private void appendOddSet(PageSet setA) throws IOException {
        writePageSets.add(new PageSet(setA));
        SetIterator itrA = new SetIteratorImpl(setA);
        while(itrA.hasNext()){
            addSortedKey(itrA.getNext());
        }

    }

    private void writeKeyToCursor(long[] key, PageProxyCursor cursor){
        for(long val : key){
            cursor.putLong(val);
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

    private void sortEachPage() throws IOException {
        long lastPage = writePageSets.getLast().peek();
        writePageSets.clear();
        int pages_per_round = 8;
        try(PageProxyCursor cursor = writeToDisk.getCursor(0, PagedFile.PF_EXCLUSIVE_LOCK)){
            for(long i = 0; i <= lastPage; i+= Math.min(pages_per_round, ((i + pages_per_round) % lastPage))){
                cursor.next(i);
                sortKeysOfPage(cursor, pages_per_round);
            }
        }
    }

    private void sortKeysOfPage(PageProxyCursor cursor, int pages_per_round) throws IOException {
        PageSet pageSet = new PageSet();
        ArrayList<Long[]> list = new ArrayList<>();
        cursor.setOffset(0);
        long firstPage = cursor.getCurrentPageId();
        for(int page = 0; page < pages_per_round; page++) {
            cursor.next(firstPage + page);
            while(!pageIsFull(cursor)){
                Long[] key = new Long[keySize];
                for (int i = 0; i < keySize; i++) {
                    key[i] = cursor.getLong();
                }
                if (key[0] != 0) {
                    list.add(key);
                }
            }
        }
        Collections.sort(list, KeyImpl.getComparator());

        cursor.next(firstPage);

        int currKey = 0;
        long currentPage = firstPage;
        while(currKey < list.size()){
            if(pageIsFull(cursor)){
                pageSet.add(currentPage);
                cursor.next(++currentPage);
                cursor.setOffset(0);
            }
            Long[] key = list.get(currKey++);
            for (Long val : key) {
                cursor.putLong(val);
            }
        }
        pageSet.add(currentPage);
        writePageSets.add(pageSet);
    }

    public void addUnsortedKey(long[] key) throws IOException {
        if(pageIsFull(writeToCursor)){
            incrementCursorToNextPage(writeToCursor);
        }
        for (long aKey : key) {
            writeToCursor.putLong(aKey);
        }
    }

    public void addSortedKey(long[] key) throws IOException {
        //System.out.println(Arrays.toString(key));
        if(pageIsFull(writeToCursor)){
            writeToCursor.next(writeToCursor.getCurrentPageId() + 1);
        }
        for (long aKey : key) {
            writeToCursor.putLong(aKey);
        }
    }

    private void incrementCursorToNextPage(PageProxyCursor cursor) throws IOException {
        long currentPage = cursor.getCurrentPageId();
        cursor.next(++currentPage);
        writePageSets.add(new PageSet(currentPage));
    }

    private boolean pageIsFull(PageProxyCursor cursor){
        return (cursor.getOffset() + keyByteSize) >= DiskCache.PAGE_SIZE;
    }

    public class SetIteratorImpl implements SetIterator{
        boolean setExhausted = false;
        PageSet set;
        byte[] byteRep = new byte[DiskCache.PAGE_SIZE];
        LongBuffer buffer = ByteBuffer.wrap(byteRep).asLongBuffer();
        long[] next = new long[keySize];

        public SetIteratorImpl(PageSet set) throws IOException {
            this.set = set;
            fillBuffer(set.pop());
            readKey();
        }

        private void fillBuffer(long pageId) throws IOException {
            if(setIteratorCursor != null) {
                setIteratorCursor.setOffset(0);
                setIteratorCursor.next(pageId);
                setIteratorCursor.getBytes(byteRep);
            }
            else{
                try(PageProxyCursor cursor = readFromDisk.getCursor(pageId, PagedFile.PF_SHARED_LOCK)){
                    cursor.getBytes(byteRep);
                }
            }
            buffer.position(0);
        }

        /*
        private void fillBuffer(long pageId) throws IOException {
            try(PageProxyCursor cursor = readFromDisk.getCursor(pageId, PagedFile.PF_SHARED_LOCK)){
                cursor.getBytes(byteRep);
            }
            buffer.position(0);
        }
        */

        public long[] getNext() throws IOException {
            long[] ret = new long[keySize];
            System.arraycopy(next, 0, ret, 0, keySize);
            readKey();
            return ret;
        }
        public long[] peekNext(){
            return next;
        }

        public boolean hasNext(){
            if(setExhausted){
                return false;
            }
            return !nextKeyAllZeros();
        }

        private boolean nextKeyAllZeros(){
            boolean zeros = false;
            for(long val : next){
                zeros = zeros || (val == 0l);
            }
            return zeros;
        }

        private void readKey() throws IOException {
            if((buffer.position() + keySize) >= buffer.capacity()){
                if(set.isEmpty()){
                    setExhausted = true;
                }
                else{
                    fillBuffer(set.pop());
                    for (int i = 0; i < keySize; i++) {
                        next[i] = buffer.get();
                    }
                }
            }
            else {
                for (int i = 0; i < keySize; i++) {
                    next[i] = buffer.get();
                }
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
