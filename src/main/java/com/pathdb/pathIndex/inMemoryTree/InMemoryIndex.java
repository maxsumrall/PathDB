/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathIndex.inMemoryTree;

import com.pathdb.pathIndex.PathIndex;
import com.pathdb.pathIndex.models.ImmutablePathPrefix;
import com.pathdb.pathIndex.models.Node;
import com.pathdb.pathIndex.models.Path;
import com.pathdb.pathIndex.models.PathInterface;
import com.pathdb.pathIndex.models.PathPrefix;
import com.pathdb.statistics.InMemoryStatisticsStore;
import com.pathdb.statistics.StatisticsStoreReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

class InMemoryIndex implements PathIndex {
    private final InMemoryStatisticsStore statisticsStore;
    private TreeMap<PathInterface, PathInterface> treeMap;

    InMemoryIndex(InMemoryStatisticsStore statisticsStore) {
        this.statisticsStore = statisticsStore;
        treeMap = new TreeMap<>();
    }

    @Override
    public Iterable<Path> getPaths(PathPrefix pathPrefix) throws IOException {
        return treeMap.subMap(pathPrefix, nextIncrementalPath(pathPrefix))
                        .values()
                        .stream()
                        .map(p -> (Path) p)
                ::iterator;
    }

    @Override
    public void insert(Path path) {
        treeMap.put(path, path);
        statisticsStore.incrementCardinality(path.getPathId(), 1);
    }

    @Override
    public StatisticsStoreReader getStatisticsStore() {
        return statisticsStore;
    }

    private PathPrefix nextIncrementalPath(PathPrefix pathPrefix) {
        long pathId = pathPrefix.getPathId();
        List<Node> nodes =
                pathPrefix
                        .getNodes()
                        .stream()
                        .map(node -> new Node(node.getId()))
                        .collect(
                                Collectors.toCollection(
                                        () -> new ArrayList<>(pathPrefix.getNodes().size())));

        if (pathPrefix.getNodes().size() == 0) {
            return ImmutablePathPrefix.of(pathId + 1, nodes);
        } else {
            Node remove = nodes.remove(nodes.size() - 1);
            nodes.add(new Node(remove.getId() + 1));
        }
        return ImmutablePathPrefix.of(pathId, nodes);
    }
}
