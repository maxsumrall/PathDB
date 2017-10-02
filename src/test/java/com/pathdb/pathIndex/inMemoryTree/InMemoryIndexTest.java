/**
 * Copyright (C) 2015-2017 - All rights reserved.
 * This file is part of the pathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.pathdb.pathIndex.inMemoryTree;

import com.pathdb.pathIndex.models.ImmutablePath;
import com.pathdb.pathIndex.models.ImmutablePathPrefix;
import com.pathdb.pathIndex.models.Node;
import com.pathdb.pathIndex.models.Path;
import com.pathdb.statistics.InMemoryStatisticsStore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

public class InMemoryIndexTest {
    @Test
    public void simpleInMemoryTest() throws Exception {
        // given
        InMemoryIndex inMemoryIndex = new InMemoryIndex(new InMemoryStatisticsStore());
        List<Node> nodes = new ArrayList<>();
        nodes.add(new Node(1));
        nodes.add(new Node(2));
        nodes.add(new Node(3));
        Path path = ImmutablePath.builder().pathId(42).addAllNodes(nodes).build();

        // when
        inMemoryIndex.insert(path);

        // then
        Iterable<Path> paths =
                inMemoryIndex.getPaths(
                        ImmutablePathPrefix.builder().pathId(42).addAllNodes(emptyList()).build());
        Path next = paths.iterator().next();

        assertEquals(path, next);
    }

    @Test
    public void returnsAllPathsOfSameId() throws Exception {
        // given
        InMemoryIndex index = createTestDatabaseA();

        // when
        Iterable<Path> pathsId0 =
                index.getPaths(
                        ImmutablePathPrefix.builder().pathId(0).addAllNodes(emptyList()).build());

        Iterable<Path> pathsId42 =
                index.getPaths(
                        ImmutablePathPrefix.builder().pathId(0).addAllNodes(emptyList()).build());

        Iterable<Path> pathsId56 =
                index.getPaths(
                        ImmutablePathPrefix.builder().pathId(0).addAllNodes(emptyList()).build());

        Iterable<Path> pathsId99 =
                index.getPaths(
                        ImmutablePathPrefix.builder().pathId(0).addAllNodes(emptyList()).build());

        // then
        ArrayList<Path> results = new ArrayList<>();
        pathsId0.forEach(results::add);
        assertEquals(100, results.size());

        results.clear();
        pathsId42.forEach(results::add);
        assertEquals(100, results.size());

        results.clear();
        pathsId56.forEach(results::add);
        assertEquals(100, results.size());

        results.clear();
        pathsId99.forEach(results::add);
        assertEquals(100, results.size());
    }

    @Test
    public void returnsAllPathsOfSamePathPrefix() throws Exception {
        // given
        InMemoryIndex index = createTestDatabaseB();
        List<Node> nodes = new ArrayList<>(1);
        nodes.add(new Node(7));

        // when
        Iterable<Path> pathsId0 =
                index.getPaths(ImmutablePathPrefix.builder().pathId(0).addAllNodes(nodes).build());

        Iterable<Path> pathsId42 =
                index.getPaths(ImmutablePathPrefix.builder().pathId(0).addAllNodes(nodes).build());

        Iterable<Path> pathsId56 =
                index.getPaths(ImmutablePathPrefix.builder().pathId(0).addAllNodes(nodes).build());

        Iterable<Path> pathsId99 =
                index.getPaths(ImmutablePathPrefix.builder().pathId(0).addAllNodes(nodes).build());

        // then
        ArrayList<Path> results = new ArrayList<>();
        pathsId0.forEach(results::add);
        assertEquals(10, results.size());

        results.clear();
        pathsId42.forEach(results::add);
        assertEquals(10, results.size());

        results.clear();
        pathsId56.forEach(results::add);
        assertEquals(10, results.size());

        results.clear();
        pathsId99.forEach(results::add);
        assertEquals(10, results.size());
    }

    public InMemoryIndex createTestDatabaseA() {
        InMemoryIndex inMemoryIndex = new InMemoryIndex(new InMemoryStatisticsStore());
        for (int i = 0; i < 1000; i++) {
            for (int j = 0; j < 100; j++) {
                List<Node> nodes = new ArrayList<>();
                nodes.add(new Node(j));
                nodes.add(new Node(j));
                nodes.add(new Node(j));
                inMemoryIndex.insert(ImmutablePath.builder().pathId(i).addAllNodes(nodes).build());
            }
        }
        return inMemoryIndex;
    }

    public InMemoryIndex createTestDatabaseB() {
        InMemoryIndex inMemoryIndex = new InMemoryIndex(new InMemoryStatisticsStore());
        for (int i = 0; i < 1000; i++) {
            for (int j = 0; j < 100; j++) {
                List<Node> nodes = new ArrayList<>();
                nodes.add(new Node(j % 10));
                nodes.add(new Node(j));
                nodes.add(new Node(j));
                inMemoryIndex.insert(ImmutablePath.builder().pathId(i).addAllNodes(nodes).build());
            }
        }
        return inMemoryIndex;
    }
}
