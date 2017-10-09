/**
 * Copyright (C) 2015-2017 - All rights reserved. This file is part of the pathdb project which is
 * released under the GPLv3 license. See file LICENSE.txt or go to
 * http://www.gnu.org/licenses/gpl.txt for full license details. You may use, distribute and modify
 * this code under the terms of the GPLv3 license.
 */
package com.pathdb.pathIndex.persisted;

import static com.jakewharton.byteunits.BinaryByteUnit.GIBIBYTES;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

import com.pathdb.pathIndex.PathIndex;
import com.pathdb.pathIndex.models.ImmutablePath;
import com.pathdb.pathIndex.models.ImmutablePathPrefix;
import com.pathdb.pathIndex.models.Node;
import com.pathdb.pathIndex.models.Path;
import com.pathdb.pathIndex.models.PathPrefix;
import java.io.File;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * This class serves as a quick-start guide for using PathDB in your own java project. The interface
 * is quite simple. This tutorial should be all you need to get started.
 */
public class TutorialTest {

    //Define a directory for where the DB files of PathDB will be stored.
    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void tutorial() throws Exception {
        //Define a directory for where the DB files of PathDB will be stored.
        File directory = tmp.newFolder("pathdb");

        PathIndex pathDB =
                new LMDBIndexFactory(directory) //The directory where
                        .withMaxDBSize(2, GIBIBYTES) //The maximum size of the path index.
                        // You should over-estimate this.
                        .build();

        //With the PathIndex object, you can insert or query from the db.
        //You insert Path objects into the db.
        //Paths consist of a path id and some nodes.
        //How you create pathID's and lists of nodes is specific to your application.
        long pathid = 42;
        List<Node> nodes = IntStream.range(1, 4).mapToObj(Node::new).collect(toList());

        //Create a path from the pathId and Nodes
        Path path = ImmutablePath.of(pathid, nodes);

        //Insert path into the path index.
        pathDB.insert(path);

        /*
         * Retrieving paths from the index is done using a path prefix. A path prefix is a partial
         * description of a path. In the minimal case, the path prefix can consist of only a pathID,
         * which will return all paths with that path ID. Adding nodes to the path prefix will
         * return query results with paths with that path ID and those nodes which exist in the
         * corresponding index along the path.
         *
         * For example, the index containing two paths,
         * Path(42, Node(1), Node(2)) and Path(42, Node(3), Node(4)), when queried with
         * the path prefix PathPrefix(42) will return both paths. When queried with the path prefix
         * PathPrefix(42, Node(1)), it will return only the first path.
         */

        //The path prefix with only the path ID.
        PathPrefix pathPrefixOnlyPathID = ImmutablePathPrefix.of(42, emptyList());

        PathPrefix pathPrefixOneNode =
                ImmutablePathPrefix.builder().pathId(42).addNodes(new Node(1)).build();

        //Querying the index returns a iterable of the found paths.
        Iterable<Path> paths = pathDB.getPaths(pathPrefixOnlyPathID);

        for (Path result : paths) {
            System.out.println(result);
        }
    }
}
