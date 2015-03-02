package bptree;import java.util.Comparator;

/**
 * Created by max on 2/12/15.
 * TODO Get rid of this class to make everything faster. Instead, promote the vals array to be what keys[] are.
 */

public class Key implements Comparator<long[]> {

    public long[] vals = new long[5];
    public Key(long[] v){
        this.vals = v;
    }
    /**
     * Given two long[]'s, which represent the full path, compare them.
     * First sort by edge labels,
     * then sort by node ids.
     *
     * @param a
     * @param b
     * @return a negative integer, zero, or a positive integer if the first argument is less than, equal to, or greater than the second.
     */
    public int compare(long[] a, long[] b) {

        //TODO Account for variable length keys. Meaning, A and B might be different lengths.
        assert (a.length == b.length);
        long temp = 0l;
        for (int i = 0; i < a.length; i++) {
            temp = a[i] - b[i];
            if (temp != 0) {
                return (a[i] < b[i]) ? -1 : 1;
            }
        }
        return 0;
    }
    public int compare(Key a, Key b) {
        if(a == null && b == null){return 0;}
        if(a == null){return -1;}
        if(b == null){ return 1;}
        return compare(a.vals, b.vals);
    }

    public int compareTo(Key b){
        return compare(this, b);
    }

    public String toString(){
        String strRep = "[";
        for(long v : vals){
            strRep += v + ",";
        }
        strRep += "]";
        return strRep;
    }
}
