package NeoIntegration;

import bptree.PageProxyCursor;
import bptree.impl.DiskCache;
import bptree.impl.NodeTree;
import bptree.impl.SearchCursor;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.IOException;

/**
 * Created by max on 5/28/15.
 */
public class LUBMExperiments {
    public NodeTree index;
    public DiskCache pathIndexDisk;
    public GraphDatabaseService database;
    public GlobalGraphOperations ggo;

    public static void main(String[] args) throws IOException {
        LUBMExperiments experiments = new LUBMExperiments();
        experiments.indexA();
        experiments.queryA();
        experiments.pathIndexDisk.shutdown();
    }

    public LUBMExperiments() throws IOException {
        pathIndexDisk = DiskCache.persistentDiskCache(BulkLUBMDataLoader.LUBM_INDEX_PATH);

        index = new NodeTree(38149, pathIndexDisk);
//28982 = first block
        database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(BulkLUBMDataLoader.DB_PATH).newGraphDatabase();
        ggo = GlobalGraphOperations.at(database);
    }

    public void queryA(){
        try(Transaction tx = database.beginTx()){
            for(RelationshipType each : ggo.getAllRelationshipTypes()){
                //System.out.println(each);
            }
            System.out.println("Begin Neo4j Transaction");
            long startTime = System.nanoTime();
            Result queryAResult = database.execute("MATCH (x)-[:takesCourse]->(y)<-[:teacherOf]-(z) RETURN ID(x), ID(y), ID(z)");
            queryAResult.next();
            long timeToFirstResult = System.nanoTime();
            //System.out.println(queryAResult.getQueryStatistics());
            //System.out.println(queryAResult.getExecutionPlanDescription());

            int count = 0;

            while(queryAResult.hasNext()){
                //System.out.println(queryAResult.next().toString());
                queryAResult.next();
                count++;
            }
            long timeToLastResult = System.nanoTime();
            System.out.println("Number of results found in Neo4j:" + count);
            System.out.println("Neo4j - Time to first result(ms): " + (timeToFirstResult - startTime) / 1000000);
            System.out.println("Neo4j - Time to last result(ms): " + (timeToLastResult - startTime) / 1000000);
        }
    }

    public void indexA() throws IOException {
        long startTime = System.nanoTime();
        long timeToFirstResult;
        long timeToLastResult;
        PathIDBuilder pathIDBuilder = (new PathIDBuilder("takesCourse", "teacherOf"));
        long pathID = pathIDBuilder.buildPath();
        //from last run: memberOfSubOrganizationOf = 90603815
        //takesCourseteacherOf --> : 1050811698
        System.out.println("Index Searching Path: " + pathIDBuilder.path.toString());
        System.out.println("Path ID searching for: " + pathID);
        long[] searchKey = new long[]{pathID};

        long[] foundKey;
        int count = 0;
        SearchCursor searchCursor = index.find(searchKey);
        try (PageProxyCursor cursor = pathIndexDisk.getCursor(searchCursor.pageID, PagedFile.PF_EXCLUSIVE_LOCK)) {
            searchCursor.next(cursor);
            timeToFirstResult = System.nanoTime();
            while(searchCursor.hasNext(cursor)) {
                foundKey = searchCursor.next(cursor);
                count++;
            }
            timeToLastResult = System.nanoTime();
        }
        System.out.println("Number of results found in Index: " + count);
        System.out.println("Index - Time to first result(ms): " + (timeToFirstResult - startTime) / 1000000);
        System.out.println("Index - Time to last result(ms): " + (timeToLastResult - startTime) / 1000000);

    }
}
