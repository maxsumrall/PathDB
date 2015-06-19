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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by max on 6/18/15.
 */
public class LUBMWorkloadQueries {
    public HashMap<Integer, NodeTree> indexes = new HashMap<>();
    public HashMap<Integer, DiskCache> disks = new HashMap<>();
    public GraphDatabaseService database;
    public GlobalGraphOperations ggo;
    StringBuilder stringBuilder;
    HashMap<Long, PathIDBuilder> relationshipMap = new HashMap<>(); //relationship types to path ids
    HashMap<long[], Long> k2PathIds = new HashMap<>();
    long k2PathCounter = 1;
    String cypher;
    Sorter k1Sorter = new Sorter(3);
    long duration = 0;

    public static void main(String[] args) throws IOException {
        LUBMWorkloadQueries workload = new LUBMWorkloadQueries();
        workload.run();
        workload.disks.get(1).shutdown();
        workload.disks.get(2).shutdown();
        workload.disks.get(3).shutdown();
    }

    public LUBMWorkloadQueries() throws IOException {

        DiskCache diskK2 = DiskCache.temporaryDiskCache("workloadK2.db", false);
        DiskCache diskK3 = DiskCache.temporaryDiskCache("workloadK3.db", false);

        disks.put(2, diskK2);
        disks.put(3, diskK3);

        NodeTree indexK2 = new NodeTree(4, diskK2);
        NodeTree indexK3 = new NodeTree(5, diskK3);
        indexes.put(2, indexK2);
        indexes.put(3, indexK3);



        stringBuilder = new StringBuilder();

        database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(CleverIndexBuilder.DB_PATH).newGraphDatabase();

        ggo = GlobalGraphOperations.at(database);
    }


    public void run() throws IOException {
        //Start query 1

        loadK1Index();

        query4();

        query5();

        query6();



    }

    public void query4() throws IOException {
        long pathID = 939155463l; //takesCourse->
        long pathID2 = 1653142233l; //<-teacherOf
        duration = 0;

        long starttime = System.nanoTime();
        long pathID3 = joinK1Results(pathID, pathID2);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;
        System.out.println("Query 4: Total : " + nanoToMilli(total));
        System.out.println("Total without insertion : " + nanoToMilli(totalWithoutInsertion));
    }

    public void query5() throws IOException {
        long pathID = 649439727l; //memberOf - >
        long pathID2 = 1522104310l; //<-subOrganizationOf
        duration = 0;

        long starttime = System.nanoTime();
        long pathID3 = joinK1Results(pathID, pathID2);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;
        System.out.println("Query 5: Total : " + nanoToMilli(total));
        System.out.println("Total without insertion : " + nanoToMilli(totalWithoutInsertion));
    }

    public void query6() throws IOException {
        long pathID = 649439727l; //memberOf - >
        long pathID2 = 1190990026l; //subOrganizationOf - >
        duration = 0;

        long starttime = System.nanoTime();
        long pathID3 = joinK1Results(pathID, pathID2);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;
        System.out.println("Query 6: Total : " + nanoToMilli(total));
        System.out.println("Total without insertion : " + nanoToMilli(totalWithoutInsertion));
    }


    public void query7() throws IOException {
        long pathID = 1918060825l; // undergraduateDegreeFrom - >
        long pathID2 = 1522104310l; //<-subOrganizationOf
        long pathID3 = 2131368143l; // <-memberOf
        duration = 0;

        long starttime = System.nanoTime();
        long pathID4 = joinK1Results(pathID, pathID2);
        long endTime = System.nanoTime();
        long total = endTime - starttime;
        long totalWithoutInsertion = total - duration;
        System.out.println("Query 7: Total : " + nanoToMilli(total));
        System.out.println("Total without insertion : " + nanoToMilli(totalWithoutInsertion));
    }



    public long joinK1Results(long pathID1, long pathID2) throws IOException {
        long[] resultB;
        long clock;
        int count = 0;
        List<long[]> entries = new ArrayList<>();
        long[] key = new long[]{pathID1, pathID2};
        if(!k2PathIds.containsKey(key))
            k2PathIds.put(key, k2PathCounter++);
        long pathID3 = k2PathIds.get(key);
        try (PageProxyCursor cursor = disks.get(1).getCursor(0, PagedFile.PF_SHARED_LOCK)) {
                SearchCursor searchCursorA = indexes.get(1).find(cursor, new long[]{pathID1});
                while (searchCursorA.hasNext(cursor)) {
                    entries.add(searchCursorA.next(cursor));
                }
                for (long[] resultA : entries) {
                    SearchCursor searchCursorB = indexes.get(1).find(cursor, new long[]{pathID2, resultA[2]});
                    while (searchCursorB.hasNext(cursor)) {
                        resultB = searchCursorB.next(cursor);
                        count++;

                        //pause clock
                        clock = System.nanoTime();
                        indexes.get(2).insert(new long[]{pathID3, resultA[1], resultA[2], resultB[2]});
                        duration += System.nanoTime() - clock;
                        //start clock again
                    }
                }
            }
        System.out.println("k2 joins found: " + count);
        return pathID3;
    }

    public void loadK1Index() throws IOException {
        enumerateSingleEdges();
        k1Sorter.sort();
        indexes.put(1, buildIndex(k1Sorter));
    }

    private void enumerateSingleEdges() throws IOException {
        int count = 0;
        double totalRels;
        try ( Transaction tx = database.beginTx()) {
            totalRels = IteratorUtil.count(ggo.getAllRelationships());
            for(Relationship edge : ggo.getAllRelationships()){
                addPath(edge.getStartNode(), edge, edge.getEndNode());
                addPath(edge.getEndNode(), edge, edge.getStartNode());
                count++;
            }
        }
        System.out.println("Keys written: " + count);
    }
    private void addPath(Node node1, Relationship relationship1, Node node2) throws IOException {
        PathIDBuilder builder = new PathIDBuilder(node1, relationship1, node2);
        long[] key = new long[]{builder.buildPath(), node1.getId(), node2.getId()};
        k1Sorter.addUnsortedKey(key);
        updateStats(builder);
    }

    public NodeTree buildIndex(Sorter sorter) throws IOException {
        DiskCache sortedDisk = sorter.getSortedDisk();
        disks.put(1, sortedDisk);
        NodeBulkLoader bulkLoader = new NodeBulkLoader(sortedDisk, sorter.finalPageId(), sorter.keySize);
        NodeTree index = bulkLoader.run();
        return index;
    }

    private void updateStats(PathIDBuilder builder){
        if(!relationshipMap.containsKey(builder.buildPath())){
            relationshipMap.put(builder.buildPath(), builder);
        }
    }
    public long nanoToMilli(long time){
        return time/1000000;
    }
}
