package bptree.impl;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Implements Comparator for sorting keys.
 */
public class KeyImpl implements Comparator<long[]> {

    private Long[] composed_key;
    private Long[] labelPath;
    private Long[] nodes;
    private static KeyImpl comparator;

    public KeyImpl(Long[] labelPath, Long[] nodes){
        this.labelPath = labelPath;
        this.nodes = nodes;
        composed_key = new Long[nodes.length + 1];
    }
    public KeyImpl(){

    }

    public static KeyImpl getComparator(){
        if(comparator == null){
            comparator = new KeyImpl();
        }
        return comparator;
    }

    public Long[] getLabelPath(){
        return this.labelPath;
    }

    public Long[] getNodes(){ return this.nodes;}

    public Long[] getComposedKey(Long pathID){
        composed_key[0] = pathID;
        System.arraycopy(this.nodes, 0, composed_key, 1, this.nodes.length);
        return composed_key;
    }

    /**
     * Given two long[]'s, which represent the keys, compare them.
     * @param a key
     * @param b key
     * @return a negative integer, zero, or a positive integer if the first argument is less than, equal to, or greater than the second.
     */
    public int compare(Long[] a, Long[] b) {

        if(a.length != b.length){ return a.length - b.length; }
        for (int i = 0; i < a.length; i++) {
            if (a[i] - b[i] != 0) {
                return a[i].compareTo(b[i]);
            }
        }
        return 0;
    }
    public int compare(long[] a, long[] b) {

        if(a.length != b.length){ return a.length - b.length; }
        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return Long.compare(a[i],b[i]);
            }
        }
        return 0;
    }


    public int prefixCompare(Long[] search_key, Long[] key) {
        long temp;
        if(search_key.length != key.length){
            return search_key.length - key.length;
        }
        for (int i = 0; i < search_key.length; i++) {
            temp = search_key[i] - key[i];
            if (temp != 0) {
                return search_key[i].compareTo(key[i]);
            }
        }
        return 0;
    }
    public int prefixCompare(long[] search_key, long[] key) {
        for (int i = 0; i < search_key.length; i++) {
            if (search_key[i] - key[i] != 0) {
                return (int) (search_key[i] - (key[i]));
            }
        }
        return search_key.length - key.length;
    }

    /**
     * Given to keys a, b, determines if a is a valid prefix of b.
     * @param a prefix to check. Your search key
     * @param b full key to compare to. Your value in the index.
     * @return
     */
    public boolean validPrefix(Long[] a, Long[] b){

        if((a.length > b.length) || a.length == 0){ //The empty prefix should match nothing. You must specify atleast the path, so give me at least one item.
            return false;
        }
        for(int i = 0; i < a.length; i++){
            if(!a[i].equals(b[i])){
                return false;
            }
        }
        return true;

    }

    public boolean validPrefix(long[] a, long[] b){

        if((a.length > b.length) || a.length == 0){ //The empty prefix should match nothing. You must specify atleast the path, so give me at least one item.
            return false;
        }
        for(int i = 0; i < a.length; i++){
            if(!(a[i] == (b[i]))){
                return false;
            }
        }
        return true;
    }

    public static String toString(Long[] key){
        return Arrays.toString(key);
    }
}
