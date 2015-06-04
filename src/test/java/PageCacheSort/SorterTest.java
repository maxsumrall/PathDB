package PageCacheSort;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

/**
 * Created by max on 5/22/15.
 */
public class SorterTest {

    Sorter sorter;

    @Before
    public void setupSorter() throws IOException {
        sorter = new Sorter(4);
    }

    @Test
    public void smallestExample() throws IOException {
        int count = 1000000;
        writeUnsortedKeysToSorter(sorter, count);
        SetIterator itr = sorter.sort();
        long[] prev = new long[]{1,1,1,1};
        int readBackCount = 0;
        while(itr.hasNext()){
            //System.out.println(Arrays.toString(itr.peekNext()));
            long next[] = itr.getNext();
            assert(next[0] > prev[0]);
            assert(next[0] == prev[0]+1);
            prev = next;
            readBackCount++;
        }
        assert(readBackCount == count-2);
    }
    @Test
    public void reverseExample() throws IOException {
        int count = 1000000;
        writeReverseKeysToSorter(sorter, count);
        SetIterator itr = sorter.sort();
        long[] prev = new long[]{0,0,0,0};
        int readBackCount = 0;
        while(itr.hasNext()){
            //System.out.println(Arrays.toString(itr.peekNext()));
            long next[] = itr.getNext();
            assert(next[0] > prev[0]);
            assert(next[0] == prev[0]+1);
            prev = next;
            readBackCount++;
        }
        assert(readBackCount == count-2);
    }
    @Test
    public void randomSorting() throws IOException {
        int count = 1000000;
        writeRandomKeysToSorter(sorter, count);
        SetIterator itr = sorter.sort();
        long[] prev = new long[]{1,1,1,1};
        int readBackCount = 0;

        while(itr.hasNext()){
            //System.out.println(Arrays.toString(itr.peekNext()));
            long next[] = itr.getNext();
            assert(next[0] > prev[0]);
            prev = next;
            readBackCount++;
        }
        assert(readBackCount == count-2);
    }

    public void writeUnsortedKeysToSorter(Sorter sorter, int count) throws IOException {
        int ijk = 0;
        for(int i = 2; i < count; i++){
            long[] key = new long[4];
            if(i % 2 == 0){
                for(int j = 0; j < key.length; j++){
                    key[j] = (i + 1);
                }
            }
            else{
                for(int j = 0; j < key.length; j++){
                    key[j] = (i - 1);
                }
            }
            sorter.addUnsortedKey(key);
            ijk++;
        }
        System.out.println(ijk);
    }
    public void writeReverseKeysToSorter(Sorter sorter, int count) throws IOException {
        int ijk = 0;
        for(int i = 2; i < count; i++) {
            long[] key = new long[4];
            for (int j = 0; j < key.length; j++) {
                key[j] = count - i;
            }
            sorter.addUnsortedKey(key);
            ijk++;
        }
        System.out.println(ijk);
    }

    public void writeRandomKeysToSorter(Sorter sorter, int count) throws IOException {
        int ijk = 0;
        Random random = new Random();
        for(int i = 2; i < count; i++){
            long[] key = new long[4];
            long rnd = Math.abs(random.nextLong());
            for(int j = 0; j < key.length; j++){
                key[j] = Math.abs(random.nextLong());
            }
            sorter.addUnsortedKey(key);
            ijk++;
        }
        System.out.println(ijk);
    }



}