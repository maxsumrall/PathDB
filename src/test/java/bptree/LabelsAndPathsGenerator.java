package bptree;

import bptree.impl.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

/**
 * Created by max on 3/24/15.
 */
public class LabelsAndPathsGenerator {

    /**
     * Generates Label Paths with random relationship ids.
     * @param number_of_paths The number of paths to generate.
     * @param k The length of paths to generate.
     * @return An ArrayList containing labeled paths each of k length.
     */
    public static ArrayList<Long[]> exampleLabelPaths(int number_of_paths, int k){
        Random random = new Random();
        ArrayList<Long[]> labelPaths = new ArrayList<>();
        for(int i = 0; i < number_of_paths; i++){
            labelPaths.add(new Long[k]);
            for(int j = 0; j < k; j++){
                labelPaths.get(i)[j] = Math.abs(random.nextLong());
            }
        }
        return labelPaths;
    }

    /**
     * Generates Label Paths with random relationship ids.
     * @param number_of_paths The number of paths to generate.
     * @param minK The minimum length of paths to generate.
     * @param maxK The maximum length of paths to generate.
     * @return An ArrayList containing labeled paths each between minK and maxK length.
     */
    public static ArrayList<Long[]> exampleVariableLengthRandomLabelPaths(int number_of_paths, int minK, int maxK){
        Random random = new Random();
        ArrayList<Long[]> labelPaths = new ArrayList<>();
        for(int i = 0; i < number_of_paths; i++){
            labelPaths.add(new Long[random.nextInt(maxK - minK + 1) + minK]);
            for(int j = 0; j < labelPaths.get(i).length; j++){
                labelPaths.get(i)[j] = random.nextLong();
            }
        }
        return labelPaths;
    }

    public static ArrayList<Long[]> exampleVariableLengthLabelPaths(int number_of_paths, int minK, int maxK){
        ArrayList<Long[]> labelPaths = new ArrayList<>();
        for(int i = 0; i < number_of_paths; i++){
            labelPaths.add(new Long[(i%5) + minK]);
            for(int j = 0; j < labelPaths.get(i).length; j++){
                labelPaths.get(i)[j] = (long)i;
            }
        }
        return labelPaths;
    }

    /**
     * Generate keys ready to be inserted into the path index. Node ID's set to random values.
     * @param labelPaths The label paths to use for generating the keys.
     * @param number_of_keys The number of keys to generate
     * @return An ArrayList of keys ready to be inserted into the database.
     */
    public static ArrayList<Key> exampleRandomKeys(ArrayList<Long[]> labelPaths, int number_of_keys) {
        return exampleKeys(labelPaths, number_of_keys, true);
    }
    /**
     * Generate keys ready to be inserted into the path index.
     * Node ID's set to sequential values from 0 to the parameter number_of_keys.
     * @param labelPaths The label paths to use for generating the keys.
     * @param number_of_keys The number of keys to generate
     * @return An ArrayList of keys ready to be inserted into the database.
     */
    public static ArrayList<Key> exampleSequentialKeys(ArrayList<Long[]> labelPaths, int number_of_keys) {
        return exampleKeys(labelPaths, number_of_keys, false);
    }

    private static ArrayList<Key> exampleKeys(ArrayList<Long[]> labelPaths, int number_of_keys, boolean randomNodeIds) {
        Random random = new Random();
        ArrayList<Key> keys = new ArrayList<>();
        for (int i = 0; i < number_of_keys; i++) {
            Long[] randomPath = labelPaths.get(i%labelPaths.size());
            Long[] nodes = new Long[randomPath.length + 1];
            for (int j = 0; j < nodes.length; j++) {
                nodes[j] = randomNodeIds ? random.nextLong() : i;
            }
            keys.add(new KeyImpl(randomPath, nodes));
        }
        return keys;
    }

    public static String treeString(Tree tree) throws IOException {
        Node rootNode = tree.getNode(tree.rootNodePageID);
        StringBuilder string = new StringBuilder();
        treeStringRecursive(rootNode, tree, string);
        return string.toString();
    }
    private static void treeStringRecursive(Node node, Tree tree, StringBuilder string) throws IOException {
        nodeString(node, string);
        if(node instanceof InternalNode){
            for(Long child : ((InternalNode)node).children){
                treeStringRecursive(tree.getNode(child), tree, string);
            }
        }

    }
    public static String nodeString(Node node, StringBuilder string){
        string.append(node instanceof LeafNode ? "Leaf Node, " : "Internal Node, ").append("Node ID: ").append(node.id);
        string.append(" PrecedingNode: ").append(node.precedingNodeId).append(" FollowingNode: ").append(node.followingNodeId);
        string.append(" Keys: ");
        for(Long[] key : node.keys){
            string.append(Arrays.toString(key)).append(", ");
        }
        string.append("\n");
        if(node instanceof InternalNode){
            string.append("Children: ");
            for(Long child : ((InternalNode)node).children){
                string.append(child).append(", ");
            }
            string.append("\n");
        }
        return string.toString();
    }

    public static Long forceSearch(Tree tree, Long[] search_key) throws IOException {
         Node rootNode = tree.getNode(tree.rootNodePageID);
        return forceSearchRecursive(tree, rootNode, search_key);
    }

    private static Long forceSearchRecursive(Tree tree, Node node,  Long[] search_key) throws IOException {
        if(node instanceof InternalNode){
            for(Long childId : ((InternalNode) node).children){
                Long result = forceSearchRecursive(tree, tree.getNode(childId), search_key);
                        if(result != -1l){
                    return result;
                }
            }
        }
        else{
            for(Long[] key : node.keys){
                if(Arrays.equals(key, search_key)){
                    return node.id;
                }
            }
        }
        return -1l;
    }

}
