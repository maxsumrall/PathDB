/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathIndex project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package NeoIntegration;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;

public class IndexLoader {
    private static final String DB_PATH = "/Users/max/Desktop/datasets/recommendations/graph.db";
    String resultString;
    String columnsString;
    String nodeResult;
    String rows = "";

    public static void main(String[] args) throws IOException {

    }
    void run()
    {
        // START SNIPPET: addData
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );


        long[] key = new long[4];
        // START SNIPPET: execute
        try ( Transaction ignored = db.beginTx();
              Result result = db.execute( "match path = (x)-[r1]-(y)-[r2]-(z) return extract(n in nodes(path) | ID(n)), extract(r in relationships(path) | ID(r)) limit 25" ) )
        {
            while ( result.hasNext() )
            {

                Map<String,Object> row = result.next();
                int i = 0;
                Object path;
                Object nodes;
                for ( Map.Entry<String,Object> column : row.entrySet() )
                {
                    if(i == 0){
                        path = column.getValue();
                    }
                    else{
                        nodes = column.getValue();
                    }
                    i++;
                    if(i >1){
                        i = 0;
                    }

                    rows += column.getKey() + ": " + column.getValue() + "; ";
                }
                rows += "\n";
            }
        }
        System.out.println(rows);
        // END SNIPPET: execute
        // the result is now empty, get a new one
        try ( Transaction ignored = db.beginTx();
              Result result = db.execute( "match (n {name: 'my node'}) return n, n.name" ) )
        {
            // START SNIPPET: items
            Iterator<Node> n_column = result.columnAs( "n" );
            for ( Node node : IteratorUtil.asIterable(n_column) )
            {
                nodeResult = node + ": " + node.getProperty( "name" );
            }
            // END SNIPPET: items

            // START SNIPPET: columns
            List<String> columns = result.columns();
            // END SNIPPET: columns
            columnsString = columns.toString();
            resultString = db.execute( "match (n {name: 'my node'}) return n, n.name" ).resultAsString();
        }

        db.shutdown();
    }



}
