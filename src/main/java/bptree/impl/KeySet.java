package bptree.impl;

import java.util.*;

/**
 * Maybe in the future I can use to instead of giving nodes a reference to Keys of type long and having to change it all the time,
 * this can be used to mask how the key set is implemented.
 */
public class KeySet implements Iterable<Long[]>, Comparator<long[]>, Comparable<long[]>{
    private LinkedList<Long[]> keys;
    private KeyImpl keyComparator = new KeyImpl(null, null);

    public KeySet(){
        keys = new LinkedList<>();
    }

    public KeySet(List<Long[]> newList){
        keys = new LinkedList<>(newList);
    }

    public int size(){
        return keys.size();
    }

    public KeySet subList(int fromIndex, int toIndex){
        return new KeySet((keys.subList(fromIndex, toIndex)));
    }

    @Override
    public Iterator<Long[]> iterator() {
        return keys.iterator();
    }

    public void sort(){
        Collections.sort(keys, keyComparator);
    }

    @Override
    public int compare(long[] o1, long[] o2) {
        return 0;
    }

    @Override
    public int compareTo(long[] o) {
        return 0;
    }
}
