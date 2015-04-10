package bptree.impl;

import bptree.Cursor;
import bptree.Index;
import bptree.Key;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Use this class for all interaction with the index.
 */
public class PathIndexImpl implements Index, Closeable, Serializable, ObjectInputValidation{

    public static final String DEFAULT_INDEX_FILE_NAME = "path_index.bin";
    private boolean signatures_specified = false;
    private boolean paths_mapping_specified = false;
    private boolean k_values_specified = false;
    private HashMap<Long[], Long> labelPathMapping = new HashMap<>();
    private List<Integer[]> signatures;
    private int minimum_k_value_indexed;
    private int maximum_k_value_indexed;
    private final String path_to_tree;
    public transient TreeImpl tree; //transient means 'do not serialize this'

    private PathIndexImpl(File file) throws IOException {
        path_to_tree = file.getName();
    }

    /**
     * Initializes an index which will be deleted after the virtual machine terminates.
     * @return A Path Index
     */
    public static Index getTemporaryPathIndex() throws IOException {
        File file = getUniqueFile();
        file.deleteOnExit();
        PathIndexImpl index = new PathIndexImpl(file);
        index.tree = TreeImpl.initializeNewTree();
        return index;
    }

    /**
     * Initializes an index which will not be deleted after the virtual machine terminates.
     * @return A Path Index
     */
    public static Index getPersistentPathIndex() throws IOException {
        PathIndexImpl index = new PathIndexImpl(getUniqueFile());
        DiskCacheImpl diskCache = DiskCacheImpl.defaultDiskCache();
        index.tree = TreeImpl.initializeNewTree(TreeImpl.DEFAULT_TREE_FILE_NAME, diskCache);
        return index;
    }

    /**
     * Loads an index from a specified path
     * @return An instantiated PathIndex as found from this file location.
     */
    public static Index loadPathIndex(String filepath_to_index) throws IOException {
        FileInputStream fis = new FileInputStream(filepath_to_index);
        byte[] bytes = new byte[fis.available()];
        fis.read(bytes);
        PathIndexImpl pathIndex;
        try {
            pathIndex = deserialize(bytes);
        }
        catch (InvalidClassException e){
            throw new InvalidClassException("Invalid object found at file: " + filepath_to_index);
        }
        pathIndex.tree = TreeImpl.loadTreeFromFile(pathIndex.path_to_tree); //TODO make sure this works
        return pathIndex;
    }

    /**
     *Returns a File object on an unused path name for the Tree, as reported by the File.exists() method.
     */
    private static File getUniqueFile(){
        File file = new File(TreeImpl.DEFAULT_TREE_FILE_NAME);
        while(file.exists()){
            file = new File(System.currentTimeMillis() + "_" + TreeImpl.DEFAULT_TREE_FILE_NAME);
        }
        return file;
    }

    /**
     * Builds a mapping for each labeled path to an integer.
     * A labeled path is a k-length list of relationship id's
     * @param labelPaths A list of labeled paths
     * @return This path index with the labeledPathMapping set.
     */
    public Index setLabelPaths(List<Long[]> labelPaths){
        labelPaths.sort(AbstractNode.keyComparator);
        for(int i = 0; i < labelPaths.size(); i++){
            labelPathMapping.put(labelPaths.get(i), (long) i);
        }
        paths_mapping_specified = true;
        return this;
    }

    /**
     * Sets the signatures for the keys for each k length.
     * All keys of the same k-length share a signature.
     * If unsure what to set it to, set it to the default signature.
     * @param newSignatures
     * @return This path index with the signatures set.
     */
    public Index setSignatures(List<Integer[]> newSignatures){
        verifySignatureIsValidForCurrentPaths(newSignatures);
        signatures = newSignatures;
        signatures_specified = true;
        return this;
    }

    private boolean verifySignatureIsValidForCurrentPaths(List<Integer[]> newSignatures){
        if (!k_values_specified){
            throw new IllegalStateException("K values are not set first. Set the k value first when building the index");
        }
        if (newSignatures.size() != maximum_k_value_indexed + 1){
            throw new IllegalStateException("Length of signatures incorrect. Length: "
                    + newSignatures.size()
                    + " Expected: " + (maximum_k_value_indexed + 1));
        }
        for(int k = minimum_k_value_indexed; k < maximum_k_value_indexed; k++){
            if (newSignatures.get(k).length != k + 1){
                throw new IllegalArgumentException("Signatures not correctly specified on signature: " + Arrays.toString(getSignature(k)));
            }
        }
        return true;
    }

    /**
     * Sets the signatures for this path index to the default values.
     */
    public Index setSignaturesToDefault(){
        signatures = getDefaultSignatures();
        return this;
    }

    /**
     * Helper method to make it more clear between which K value is which index.
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
     * @param minK The minimum k value in this path index
     * @param maxK The maximum k value in this path index
     * @return the default signatures for specified k values.
     */
    private List<Integer[]> getDefaultSignatures(int minK, int maxK){
        ArrayList<Integer[]> defaultSignatures = new ArrayList<>();
        for (int k = 0; k < maxK + 1; k++){ //maxK + 1 so that the index for k = 2 is still .get(2)
            defaultSignatures.add(new Integer[k + 1]);
            for (int i = 0; i < k + 1; i++) {
                defaultSignatures.get(k)[i] = i;
            }
        }
        return defaultSignatures;
    }
    /**
     * This method returns the default signatures. Uses the k values already specified.
     * For minK = 2 and maxK = 4, this data structure looks like:
     * [[0, 1, 2] , [0, 1, 2, 3], [0, 1, 2, 3, 4]]
     *
     * If you don't know what to make the signature, then you should be using this.
     *
     * @return the default signatures for specified k values.
     */
    public List<Integer[]> getDefaultSignatures(){
        ArrayList<Integer[]> defaultSignatures = new ArrayList<>(maximum_k_value_indexed - minimum_k_value_indexed);
        for (int k = minimum_k_value_indexed; k < maximum_k_value_indexed; k++){
            defaultSignatures.set((k - minimum_k_value_indexed), new Integer[k + 1]);
            for (int i = 0; i < k + 1; i++) {
                defaultSignatures.get(k - minimum_k_value_indexed)[i] = i;
            }
        }
        return defaultSignatures;
    }

    /**
     * Sets the minimum and maximum k values that are indexed by this path index.
     * @param minK The minimum k value to be indexed.
     * @param maxK The maximum k value to be indexed.
     * @return This path index with the k values set.
     */
    public Index setRangeOfPathLengths(int minK, int maxK){
        minimum_k_value_indexed = minK;
        maximum_k_value_indexed = maxK;
        k_values_specified = true;
        return this;
    }

    /**
     * Checks if the necessary variables have been set.
     * @return true if signatures, path mapping, and k values have been set.
     */
    public boolean ready(){
        return signatures_specified && paths_mapping_specified && k_values_specified;
    }

    /**
     * Returns an iterable cursor object
     */
    public Cursor find(Key key) {
        try {
            Long[] search_key = buildComposedKey(key);
            return tree.find(search_key);
        }
        catch (IOException e){
            return null; //TODO something better here
        }
    }

    /**
     * Inserts a key into the index.
     */
    public void insert(Key key) throws IOException {
        Long[] search_key = buildComposedKey(key);
        tree.insert(search_key);
    }

    /**
     * Searches for a key in the index and removes it.
     * @return false if this key is not found.
     */
    public boolean remove(Key key){
        return true;
    }

    public Long[] buildComposedKey(Key key){
        Long pathIdForKey = labelPathMapping.get(key.getLabelPath());
        return key.getComposedKey(pathIdForKey);
    }


    /**
     * Save the state of the index to the file to load later.
     */
    public void close() throws IOException {
        FileOutputStream fos = new FileOutputStream(DEFAULT_INDEX_FILE_NAME);
        fos.write(serialize(this));
        fos.close();
    }

    /**
     * Serialize this object
     * @throws IOException
     */
    private static byte[] serialize(Object o) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(o);
        oos.flush();
        oos.close();
        return baos.toByteArray();
    }

    /**
     * Deserialize this object
     * @throws IOException
     */
    private static PathIndexImpl deserialize(byte[] bytes) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais);
        PathIndexImpl index = null;
        try{
            Object o = ois.readObject();
            ois.close();
            if (o instanceof PathIndexImpl){
                index = (PathIndexImpl) o;
            }
        }
        catch(ClassNotFoundException e){
            throw new InvalidClassException("Attempted to read invalid Path Index object from byte array");
        }
        return index;
    }
    @Override
    public void validateObject() throws InvalidObjectException {

    }

    /**
     * Methods implementing serialization
     * @throws IOException
     */
    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
    }

    /**
     * Method implementing serialization
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.registerValidation(this, 0);
        in.defaultReadObject();
    }
}
