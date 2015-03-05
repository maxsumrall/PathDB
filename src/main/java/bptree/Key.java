package bptree;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Implements Comparator for sorting keys.
 */
public class Key implements Comparator<Long[]> {

    /**
     * Given two long[]'s, which represent the keys, compare them.
     * @param a key
     * @param b key
     * @return a negative integer, zero, or a positive integer if the first argument is less than, equal to, or greater than the second.
     */
    public int compare(Long[] a, Long[] b) {

        if(a.length != b.length){ return a.length - b.length; }
        long temp;
        for (int i = 0; i < a.length; i++) {
            temp = a[i] - b[i];
            if (temp != 0) {
                return (a[i] < b[i]) ? -1 : 1;
            }
        }
        return 0;
    }

    public static String asString(Long[] key){
        return Arrays.toString(key);
    }
}
