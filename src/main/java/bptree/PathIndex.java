package bptree;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Use this class for all interaction with the index.
 */
public class PathIndex implements Closeable, Serializable, ObjectInputValidation{

    public static final String DEFAULT_INDEX_FILE_NAME = "path_index.bin";
    private final boolean signatures_specified = false;
    private boolean paths_mapping_specified = false;
    private boolean k_values_specified = false;
    private HashMap<Long[], Long> labelPathMapping = new HashMap<>();
    private ArrayList<Integer[]> signatures;
    private int minimum_k_value_indexed;
    private int maximum_k_value_indexed;
    private final String path_to_tree;
    private transient Tree tree; //transient means 'do not serialize this'

    private PathIndex(File file) throws IOException {
        path_to_tree = file.getName();
    }

    /**
     * Initializes an index which will be deleted after the virtual machine terminates.
     * @return A Path Index
     */
    public static PathIndex temporaryPathIndex() throws IOException {
        File file = getUniqueFile();
        file.deleteOnExit();
        PathIndex index = new PathIndex(file);
        index.tree = Tree.initializeNewTree();
        return index;
    }

    /**
     * Initializes an index which will not be deleted after the virtual machine terminates.
     * @return A Path Index
     */
    public static PathIndex savedPathIndex() throws IOException {
        PathIndex index = new PathIndex(getUniqueFile());
        index.tree = Tree.initializeNewTree(Tree.DEFAULT_CACHE_FILE_NAME, Tree.DEFAULT_TREE_FILE_NAME, false);
        return index;
    }

    /**
     * Loads an index from a specified path
     * @param filepath_to_index
     * @return An instantiated PathIndex as found from this file location.
     */
    public static PathIndex loadPathIndex(String filepath_to_index) throws IOException {
        FileInputStream fis = new FileInputStream(filepath_to_index);
        byte[] bytes = new byte[fis.available()];
        fis.read(bytes);
        PathIndex pathIndex;
        try {
            pathIndex = deserialize(bytes);
        }
        catch (InvalidClassException e){
            throw new InvalidClassException("Invalid object found at file: " + filepath_to_index);
        }
        pathIndex.tree = Tree.loadTreeFromFile(pathIndex.path_to_tree); //TODO make sure this works
        return pathIndex;
    }

    /**
     *Returns a File object on an unused path name for the Tree, as reported by the File.exists() method.
     * @return
     */
    private static File getUniqueFile(){
        File file = new File(Tree.DEFAULT_TREE_FILE_NAME);
        while(file.exists()){
            file = new File(System.currentTimeMillis() + "_" + Tree.DEFAULT_TREE_FILE_NAME);
        }
        return file;
    }

    /**
     * Builds a mapping for each labeled path to an integer.
     * A labeled path is a k-length list of relationship id's
     * @param labelPaths A list of labeled paths
     * @return This path index with the labeledPathMapping set.
     */
    public PathIndex buildLabelPathMapping(ArrayList<Long[]> labelPaths){
        labelPaths.sort(new Key());
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

    /**
     * Sets the signatures for this path index to the default values.
     */
    public void setDefaultSignatures(){
        signatures = defaultSignatures();
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
    public static ArrayList<Integer[]> defaultSignatures(int minK, int maxK){
        ArrayList<Integer[]> defaultSignatures = new ArrayList<>(maxK - minK);
        for (int k = minK; k < maxK; k++){
            defaultSignatures.set((k - minK), new Integer[k + 1]);
            for (int i = 0; i < k + 1; i++) {
                defaultSignatures.get(k - minK)[i] = i;
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
    public ArrayList<Integer[]> defaultSignatures(){
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
    public PathIndex setKValues(int minK, int maxK){
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
     * @param labelPath the path to search for
     * @param nodes the nodes that may be specified in the search.
     * @return
     */
    public Long[] find(Long[] labelPath, Long[] nodes) throws IOException {
        return tree.find(build_searchKey(labelPath, nodes));
    }

    public Long[] build_searchKey(Long[] labelPath, Long[] nodes){
        Long[] search_key = new Long[(labelPath.length * 2) + 1];
        Long pathID = labelPathMapping.get(labelPath);
        search_key[0] = pathID;
        System.arraycopy(nodes, 0, search_key, 1, nodes.length - 1);
        return search_key;
    }

    /**
     * Inserts a key into the index.
     * @param labelPath The labeled path for this key.
     * @param nodes The nodes along the labeled path.
     */
    public void insert(Long[] labelPath, Long[] nodes) throws IOException {
        tree.insert(build_searchKey(labelPath, nodes));
    }

    /**
     * Searches for a key in the index and removes it.
     * @param labelPath The labeled path for this key.
     * @param nodes The nodes along this labeled path.
     * @return false if this key is not found.
     */
    public boolean remove(Long[] labelPath, Long[] nodes){
        return true;
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
     * @param o
     * @return
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
     * @param bytes
     * @return
     * @throws ClassNotFoundException
     * @throws IOException
     */
    private static PathIndex deserialize(byte[] bytes) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais);
        PathIndex index = null;
        try{
            Object o = ois.readObject();
            ois.close();
            if (o instanceof PathIndex){
                index = (PathIndex) o;
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
     * @param out
     * @throws IOException
     */
    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
    }

    /**
     * Method implementing serialization
     * @param in
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.registerValidation(this, 0);
        in.defaultReadObject();
    }
}
