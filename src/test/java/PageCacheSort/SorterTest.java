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
        writeUnsortedKeysToSorter(sorter, 1000000);
        SetIterator itr = sorter.sort();
        long[] prev = new long[]{0,0,0,0};
        while(itr.hasNext()){
            long next[] = itr.getNext();
            assert(next[0] > prev[0]);
            prev = next;
        }
    }
    @Test
    public void randomSorting() throws IOException {
        writeRandomKeysToSorter(sorter, 1000000);
        SetIterator itr = sorter.sort();
        long[] prev = new long[]{0,0,0,0};
        while(itr.hasNext()){
            long next[] = itr.getNext();
            assert(next[0] >= prev[0]);
            prev = next;
        }
    }

    public void writeUnsortedKeysToSorter(Sorter sorter, int count) throws IOException {
        long[] key = new long[4];
        for(int i = 2; i < count; i++){
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
        }
    }

    public void writeRandomKeysToSorter(Sorter sorter, int count) throws IOException {
        long[] key = new long[4];
        Random random = new Random();
        for(int i = 2; i < count; i++){
            long rnd = Math.abs(random.nextLong());
            for(int j = 0; j < key.length; j++){
                key[j] = rnd;
            }
            sorter.addUnsortedKey(key);
        }
    }



}