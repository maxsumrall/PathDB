package bptree;

import java.io.Closeable;
import java.io.File;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Use this class for all interaction with the index.
 */
public class PathIndex implements Closeable {

    public static String DEFAULT_INDEX_FILE_NAME = "path_index.bin";
    private boolean signatures_specified = false;
    private boolean paths_mapping_specified = false;
    private boolean k_values_specified = false;
    private HashMap<Long[], Long> labelPathMapping = new HashMap();
    private ArrayList<Integer[]> signatures;
    private int minimum_k_value_indexed;
    private int maximum_k_value_indexed;
    private Tree tree;

    private PathIndex(File file){

    }

    /**
     * Initializes an index which will be deleted after the virtual machine terminates.
     * @return A Path Index
     */
    public static PathIndex temporaryPathIndex(){
        File file = getUniqueFile();
        file.deleteOnExit();
        return new PathIndex(file);
    }

    /**
     * Initializes an index which will not be deleted after the virtual machine terminates.
     * @return A Path Index
     */
    public static PathIndex savedPathIndex() {
        return new PathIndex(getUniqueFile());
    }

    /**
     * Loads an index from a specified path
     * @param path_to_index
     * @return
     */
    public static PathIndex loadPathIndex(String path_to_index){
        File file = new File(path_to_index);
    }

    private static File getUniqueFile(){
        File file = new File(DEFAULT_INDEX_FILE_NAME);
        while(file.exists()){
            //throw new FileAlreadyExistsException("This Index File Already Exists: " + file.getPath());
            file = new File(System.currentTimeMillis() + "_" + DEFAULT_INDEX_FILE_NAME);
        }
        return file;
    }

    /**
     * Save the state of the index to the file to load later.
     */
    public void close(){


    }

    public PathIndex buildLabelPathMapping(ArrayList<Long[]> labelPaths){
        labelPaths.sort(new Key());
        for(int i = 0; i < labelPaths.size(); i++){
            labelPathMapping.put(labelPaths.get(i), (long) i);
        }
        paths_mapping_specified = true;
        return this;
    }

    public PathIndex setSignatures(ArrayList<Integer[]> newSignatures){
        if (!k_values_specified){
            throw new IllegalStateException("K values are not set first. Set the k value first when building the index");
        }
        if (newSignatures.size() != maximum_k_value_indexed){
            throw new IllegalStateException("Length of signatures incorrect. Length: "
                    + newSignatures.size()
                    + " Expected: " + (maximum_k_value_indexed));
        }
        for(int k = minimum_k_value_indexed; k < maximum_k_value_indexed; k++){
            if (getSignature(k).length != k + 1){
                throw new IllegalArgumentException("Signatures not correctly specified on signature: " + Arrays.toString(getSignature(k)));
            }
        }
        signatures = newSignatures;
        return this;
    }

    public void setDefaultSignatures(){
        signatures = defaultSignatures();
    }

    /**
     * Helped method to make it more clear between which K value is which index.
     * @param k value to have the signature of
     * @return the signature to use when storing/sorting a key in the index.
     */
    private Integer[] getSignature(int k){
        return signatures.get(k - minimum_k_value_indexed);
    }

    /**
     * This method returns the default signatures.
     * For minK = 2 and maxK = 4, this data structure looks like:
     * [[0, 1, 2] , [0, 1, 2, 3], [0, 1, 2, 3, 4]]
     *
     * If you don't know what to make the signature, then you should be using this.
     *
     * @param minK
     * @param maxK
     * @return the default signatures for specified k values.
     */
    public static ArrayList<Integer[]> defaultSignatures(int minK, int maxK){
        ArrayList<Integer[]> defaultSigatures = new ArrayList<>(maxK - minK);
        for (int k = minK; k < maxK; k++){
            defaultSigatures.set((k - minK), new Integer[k + 1]);
            for (int i = 0; i < k + 1; i++) {
                defaultSigatures.get(k - minK)[i] = i;
            }
        }
        return defaultSigatures;
    }
    public ArrayList<Integer[]> defaultSignatures(){
        ArrayList<Integer[]> defaultSigatures = new ArrayList<>(maximum_k_value_indexed - minimum_k_value_indexed);
        for (int k = minimum_k_value_indexed; k < maximum_k_value_indexed; k++){
            defaultSigatures.set((k - minimum_k_value_indexed), new Integer[k + 1]);
            for (int i = 0; i < k + 1; i++) {
                defaultSigatures.get(k - minimum_k_value_indexed)[i] = i;
            }
        }
        return defaultSigatures;
    }

    public PathIndex setKValues(int minK, int maxK){
        minimum_k_value_indexed = minK;
        maximum_k_value_indexed = maxK;
        k_values_specified = true;
        return this;
    }

    public boolean ready(){
        return signatures_specified && paths_mapping_specified && k_values_specified;
    }

    /**
     * Returns an iterable cursor object
     * @param labelPath the path to search for
     * @param nodes the nodes that may be specified in the search.
     * @return
     */
    public Cursor find(Long[] labelPath, Long[] nodes){
        Long[] search_key = new Long[(labelPath.length * 2) + 1];
        Long pathID = labelPathMapping.get(labelPath);
        search_key[0] = pathID;
        for (int i = 1; i < nodes.length; i++){
            search_key[i] = nodes[i - 1];
        }
        return tree.find(search_key);
    }

    public void insert(Long[] labelPath, Long[] nodes){

    }

    public void remove(Long[] key, Long[] nodes){

    }

}
