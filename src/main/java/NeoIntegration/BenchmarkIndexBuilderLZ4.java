package NeoIntegration;

import PageCacheSort.Sorter;
import bptree.PageProxyCursor;
import bptree.impl.DiskCache;
import bptree.impl.IndexBulkLoader;
import bptree.impl.IndexTree;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by max on 6/2/15.
 */
public class BenchmarkIndexBuilderLZ4 {
    public static final int MAX_K = 1;
    public static final String DB_PATH = "graph.db/";
    public static final String LUBM_INDEX_PATH = "BenchmarkLZ4lubm50Index.db";
    public static final String INDEX_METADATA_PATH = "BenchmarkLZ4MetaData.dat";
    StringBuilder strBulder;
    HashMap<Integer, Sorter> sorters = new HashMap<>();
    Map<Integer, IndexTree> indexes = new HashMap<>();
    HashMap<Long, PathIDBuilder> relationshipMap = new HashMap<>(); //relationship types to path ids
    HashMap<Long, PathIDBuilder> k2RelationshipsMap = new HashMap<>();
    HashMap<Long, PathIDBuilder> k3RelationshipsMap = new HashMap<>();
    HashMap<Long, Long> k2PathIds = new HashMap<>();
    HashMap<Long, Long> k3PathIds = new HashMap<>();
    long currentShortPathID = 1;
    LZ4DiskFiller k1LZ4DiskFiller = new LZ4DiskFiller(3);
    LZ4DiskFiller k2LZ4DiskFiller = new LZ4DiskFiller(4);
    //FillSortedDisk k2FillSortedDisk = new FillSortedDisk(4);
    //SuperFillSortedDisk superk2DiskFiller = new SuperFillSortedDisk(4);


    public static void main(String[] args) throws IOException {
        BenchmarkIndexBuilderLZ4 indexBuilder = new BenchmarkIndexBuilderLZ4();
    }

    public BenchmarkIndexBuilderLZ4() throws IOException {

        for(int i = 1; i <= MAX_K; i++){
            sorters.put(i + 2, new Sorter(i + 2));
        }

        long startTime = System.nanoTime();
        enumerateSingleEdges();
        long endTime = System.nanoTime();
        logToFile("Time to enumerate K1 edges(ns): " + (endTime - startTime));
        //Sorter sorterK1 = sorters.get(3);
        System.out.println("\nSorting K = 1");

        startTime = System.nanoTime();
        //SetIterator k1Iterator = sorterK1.sort();
        endTime = System.nanoTime();
        //logToFile("Time to sort K1 edges(ns): " + (endTime - startTime));

        startTime = System.nanoTime();
        //NodeTree k1Index = buildIndex(sorterK1);
        endTime = System.nanoTime();
        //logToFile("Time to bulk load K1 edges into index(ns): " + (endTime - startTime));

        //indexes.put(1, k1Index);

        if(MAX_K > 1) {
            startTime = System.nanoTime();
            buildK2Paths();
            //Sorter sorterK2 = sorters.get(4);
            k2LZ4DiskFiller.finish();
            k2LZ4DiskFiller.compressedDisk.shutdown();
            //SetIterator k2Iterator = sorterK2.finishWithoutSort();
            logToFile("Time to build K2 edges(ns): " + (System.nanoTime() - startTime));

            //NodeTree k2Index = buildIndex(sorterK2);
            //indexes.put(2, k2Index);
        }
    }

    public IndexTree buildIndex(Sorter sorter) throws IOException {
        System.out.println("Building Index");
        DiskCache sortedDisk = sorter.getSortedDisk();
        IndexBulkLoader bulkLoader = new IndexBulkLoader(sortedDisk, sorter.finalPageId(), sorter.keySize);
        IndexTree index = bulkLoader.run();
        File newFile = new File(sorter.toString() + LUBM_INDEX_PATH);
        sortedDisk.pageCacheFile.renameTo(new File(sorter.toString() + LUBM_INDEX_PATH));
        index.disk.pageCacheFile = newFile;
        System.out.println("Done. Root for this index: " + index.rootNodeId);
        logToFile("index K= " + sorter.keySize + " root: " + index.rootNodeId);
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
                //addPath(edge.getStartNode(), edge, edge.getEndNode());
                //addPath(edge.getEndNode(), edge, edge.getStartNode());
                k1LZ4DiskFiller.addKey(new long[]{edge.getStartNode().getId(), edge.getType().name().hashCode(), edge.getEndNode().getId()});
                k1LZ4DiskFiller.addKey(new long[]{edge.getEndNode().getId(), edge.getType().name().hashCode(), edge.getStartNode().getId()});
                count++;
            }
        }
        System.out.println("Keys written: " + count);
    }

    private void buildK2Paths() throws IOException {
        System.out.println("Building K2 Paths");
        int pathCount = 0;
        long[] combinedPath;
        ArrayList<long[]> entries = new ArrayList<>();
        int total = relationshipMap.size() * relationshipMap.size();
        try (PageProxyCursor cursorA = indexes.get(1).disk.getCursor(0, PagedFile.PF_SHARED_LOCK)) {
            for(long pathIdA : relationshipMap.keySet()){
                for(long pathIdB: relationshipMap.keySet()) {
                    System.out.print("\rPaths complete: " + pathCount++ + "/" + total);
                    entries.clear();
                    SearchCursor resultA = indexes.get(1).find(cursorA, new long[]{pathIdA});
                    while (resultA.hasNext(cursorA)) {
                        entries.add(resultA.next(cursorA));
                    }

                    for (long[] entry : entries) {
                        SearchCursor resultB = indexes.get(1).find(cursorA, new long[]{pathIdB, entry[2]});
                        while (resultB.hasNext(cursorA)) {
                            long[] secondPath = resultB.next(cursorA);

                            PathIDBuilder builder = new PathIDBuilder(relationshipMap.get(entry[0]).getPath(), relationshipMap.get(pathIdB).getPath());
                            if (!k2PathIds.containsKey(builder.buildPath())) {
                                k2PathIds.put(builder.buildPath(), currentShortPathID++);
                                k2RelationshipsMap.put(k2PathIds.get(builder.buildPath()), builder);
                            }
                            long k2PathId = k2PathIds.get(builder.buildPath());
                            combinedPath = new long[]{k2PathId, entry[1], entry[2], secondPath[2]};
                            //sorters.get(4).addSortedKeyBulk(combinedPath);
                            k2LZ4DiskFiller.addKey(combinedPath);
                            //superk2DiskFiller.addKey(combinedPath);
                            //k2FillSortedDisk.addKey(combinedPath);
                        }
                    }
                }
            }
        }
    }

    private void addPath(Node node1, Relationship relationship1, Node node2) throws IOException {
        PathIDBuilder builder = new PathIDBuilder(node1, relationship1, node2);
        long[] key = new long[]{builder.buildPath(), node1.getId(), node2.getId()};
        sorters.get(3).addUnsortedKey(key);
        updateStats(builder);
    }

    private void updateStats(PathIDBuilder builder){
        if(!relationshipMap.containsKey(builder.buildPath())){
            relationshipMap.put(builder.buildPath(), builder);
        }
    }

    public static void logToFile(String text){
        try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("index_building_times_BENCHMARK.txt", true)))) {
            out.println(text);
            System.out.println(text);
        }catch (IOException e) {
            System.out.println(e.getStackTrace());
        }
    }

}

