package bptree;

import java.io.File;
import java.nio.file.FileAlreadyExistsException;
import java.util.HashMap;

/**
 * Use this class for all interaction with the index.
 */
public class PathIndex {

    public static String DEFAULT_FILE_NAME = "path_index.bin";
    private boolean signatures_specified = false;
    private boolean paths_mapping_specified = false;
    private HashMap<Long[], Long> labelPathMapping = new HashMap();
    private int minimum_k_value_indexed;
    private int maximum_k_value_indexed;

    private PathIndex(){

    }

    public static PathIndex emptyPathIndex(File file) throws FileAlreadyExistsException {
        if(file.exists()){
            throw new FileAlreadyExistsException("This Index File Already Exists: " + file.getPath());
        }

    }

    public PathIndex buildLabelPathMapping(Long[][] labelPaths, boolean sorted){
        if(!sorted){

        }
        for(int i = 0; i < labelPaths.length; i++){
            labelPathMapping.put(labelPaths[i], (long) i);
        }
        paths_mapping_specified = true;
        return this;
    }

    public PathIndex setSignatures(int[][] signatures){
        if(minimum_k_value_indexed == 0){
            throw new IllegalStateException("K values are not set first. Set the k value first when building the index");
        }
        if(signatures.length != maximum_k_value_indexed + 1){
            throw new IllegalStateException("Length of signatures incorrect. Length: "
                    + signatures.length
                    + " Expected: " + (maximum_k_value_indexed + 1));
        }

    }

    public static PathIndex loadPathIndexFromFile(File file) {

    }

    public boolean ready(){
        return signatures_specified && paths_mapping_specified;
    }

    /**
     * Returns an iterable cursor object
     * @param key
     * @return
     */
    public IndexCursor find(Long[] key){

    }

    public void insert(Long[] key){

    }

    public void remove(Long[] key){

    }

    private class IndexCursor extends Cursor {

    }

}
