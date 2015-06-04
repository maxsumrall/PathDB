package NeoIntegration;

import PageCacheSort.Sorter;
import bptree.PageProxyCursor;
import bptree.impl.DiskCache;
import bptree.impl.NodeBulkLoader;
import bptree.impl.NodeTree;
import bptree.impl.SearchCursor;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.*;
import java.util.*;

/**
 * Created by max on 6/2/15.
 */
public class CleverIndexBuilder {
    public static final int MAX_K = 2;
    public static final String DB_PATH = "graph.db/";
    public static final String LUBM_INDEX_PATH = "Cleverlubm50Index.db";
    public static final String INDEX_METADATA_PATH = "pathIndexMetaData.dat";
    StringBuilder strBulder;
    LinkedList<String> prettyPaths = new LinkedList<>();
    HashMap<Integer, Sorter> sorters = new HashMap<>();
    Map<Integer, NodeTree> indexes = new HashMap<>();
    TreeMap<Long, PathIDBuilder> relationshipMap = new TreeMap<>(); //relationship types to path ids
    TreeMap<Long, PathIDBuilder> k2RelationshipsMap = new TreeMap<>(); //relationship types to path ids


    public static void main(String[] args) throws IOException {
        CleverIndexBuilder indexBuilder = new CleverIndexBuilder();

        try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(INDEX_METADATA_PATH, false)))) {
            for(int i = 0; i < indexBuilder.indexes.size(); i++){
                out.println(indexBuilder.sorters.get(i+3).keySize + "," + indexBuilder.indexes.get(i+1).rootNodeId);
                indexBuilder.indexes.get(i+1).disk.shutdown();
            }
        }
        try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("CleverLUBMLoaderLog" +".txt", false)))) {
            System.out.println("");
            for (PathIDBuilder builder: indexBuilder.relationshipMap.values()) {
                System.out.println("Path: " + builder.prettyPrint() + " , pathID: " + builder.buildPath());
                out.println("Path: " + builder.prettyPrint() + " , pathID: " + builder.buildPath());
            }
            for (PathIDBuilder builder: indexBuilder.k2RelationshipsMap.values()) {
                System.out.println("Path: " + builder.getPath() + " , pathID: " + builder.buildPath());
                out.println("Path: " + builder.getPath() + " , pathID: " + builder.buildPath());
            }
        }
    }

    public CleverIndexBuilder() throws IOException {

        for(int i = 1; i <= MAX_K; i++){
            sorters.put(i + 2, new Sorter(i + 2));
        }

        enumerateSingleEdges();
        Sorter sorterK1 = sorters.get(3);
        System.out.println("\nSorting K = 1");
        sorterK1.sort();
        NodeTree k1Index = buildIndex(sorterK1);
        indexes.put(1, k1Index);

        buildK2Paths();
        Sorter sorterK2 = sorters.get(4);
        //sorterK2.finishWithoutSort();
        sorterK2.sort();
        NodeTree k2Index = buildIndex(sorterK2);
        indexes.put(2, k2Index);
    }

    public NodeTree buildIndex(Sorter sorter) throws IOException {
        System.out.println("Building Index");
        DiskCache sortedDisk = sorter.getSortedDisk();
        NodeBulkLoader bulkLoader = new NodeBulkLoader(sortedDisk, sorter.finalPageId(), sorter.keySize);
        NodeTree index = bulkLoader.run();
        sortedDisk.pageCacheFile.renameTo(new File(sorter.toString() + LUBM_INDEX_PATH));
        System.out.println("Done. Root for this index: " + index.rootNodeId);
        return index;
    }

    private void printStats(double count, double totalRels){
        if(strBulder != null){
            int b = strBulder.toString().length();
            for(int i = 0; i < b; i++){System.out.print("\b");}
        }
        strBulder = new StringBuilder();
        strBulder.append("Progress: ").append(count).append("  |  ").append((int) ((count / totalRels) * 100)).append("% complete. Paths: ").append(relationshipMap.size());
        System.out.print("\r" + strBulder.toString());
    }


    private void enumerateSingleEdges() throws IOException {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
        GlobalGraphOperations ggo = GlobalGraphOperations.at(db);
        int count = 0;
        double totalRels;
        try ( Transaction tx = db.beginTx()) {
            totalRels = IteratorUtil.count(ggo.getAllRelationships());
            for(Relationship edge : ggo.getAllRelationships()){
                if (count % 1000 == 0) {
                    printStats(count, totalRels);
                }
                addPath(edge.getStartNode(), edge, edge.getEndNode());
                addPath(edge.getEndNode(), edge, edge.getStartNode());
                count++;
            }
        }
        System.out.println("Keys written: " + count);
    }

    private void buildK2Paths() throws IOException {
        System.out.println("Building K2 Paths");
        PageProxyCursor cursor;
        int pathCount = 0;
        int k2count = 0;
        for(long pathId : relationshipMap.keySet()){
            System.out.print("\rPaths complete: " + pathCount++ + "/" + relationshipMap.size());
            SearchCursor result = indexes.get(1).find(new long[]{pathId});
            cursor = indexes.get(1).disk.getCursor(result.pageID, PagedFile.PF_SHARED_LOCK);
            while(result.hasNext(cursor)){
                long[] entry = result.next(cursor);
                cursor.close();
                k2count += enumerateAllPathsFrom(entry);
                cursor = indexes.get(1).disk.getCursor(result.pageID, PagedFile.PF_SHARED_LOCK);
            }
        }
        System.out.println("Keys written: " + k2count);
    }

    private int enumerateAllPathsFrom(long[] key) throws IOException {
        int k2Count = 0;
        Long[] combinedPath;
        for(long pathId : relationshipMap.keySet()) {
            PathIDBuilder builder = new PathIDBuilder( relationshipMap.get(key[0]).getPath(), relationshipMap.get(pathId).getPath() );
            long k2PathId = builder.buildPath();
            if(!k2RelationshipsMap.containsKey(k2PathId)) {
                k2RelationshipsMap.put(k2PathId, builder);
            }
            long endNodeId = key[key.length - 1];
            long[] searchKey = new long[]{pathId, endNodeId};
            SearchCursor result = indexes.get(1).find(searchKey);
            try (PageProxyCursor cursor = indexes.get(1).disk.getCursor(result.pageID, PagedFile.PF_SHARED_LOCK)) {
                while (result.hasNext(cursor)) {
                    long[] secondPath = result.next(cursor);
                    if(key[1] == secondPath[2] && key[2] != secondPath[1]){
                        continue;
                    }
                    combinedPath = new Long[]{k2PathId, key[1], key[2], secondPath[2]};
                    sorters.get(4).addUnsortedKey(combinedPath);
                    k2Count++;
                }
            }
        }
        return k2Count;
    }

    private void addPath(Node node1, Relationship relationship1, Node node2) throws IOException {
        PathIDBuilder builder = new PathIDBuilder(node1, relationship1, node2);
        Long[] key = new Long[]{builder.buildPath(), node1.getId(), node2.getId()};
        sorters.get(3).addUnsortedKey(key);
        updateStats(builder);
    }
    private void addPath(Node node1, Relationship relationship1, Node node2, Relationship relationship2, Node node3) throws IOException {
        PathIDBuilder builder = new PathIDBuilder(node1, relationship1, node2, relationship2, node3);
        sorters.get(4).addUnsortedKey(new Long[]{builder.buildPath(), node1.getId(), node2.getId(), node3.getId()});
        updateStats(builder);
    }

    private void addPath(Node node1, Relationship relationship1, Node node2, Relationship relationship2, Node node3, Relationship relationship3, Node node4) throws IOException {
        PathIDBuilder builder = new PathIDBuilder(node1, relationship1, node2, relationship2, node3, relationship3, node4);
        sorters.get(5).addUnsortedKey(new Long[]{builder.buildPath(), node1.getId(), node2.getId(), node3.getId(), node4.getId()});
        updateStats(builder);
    }

    private void updateStats(PathIDBuilder builder){
        if(!relationshipMap.containsKey(builder.buildPath())){
            relationshipMap.put(builder.buildPath(), builder);
        }
    }

}
