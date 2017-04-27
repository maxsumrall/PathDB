PathDB
=====

<!-- TODO: Current build is not ready yet for Travis, enable this batch again after refactoring is done -->
[![Build Status](https://travis-ci.org/maxsumrall/PathDB.svg?branch=master)](https://travis-ci.org/maxsumrall/PathDB)
[![Maven Central](https://img.shields.io/maven-central/v/com.pathdb/pathdb.svg?maxAge=3600)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.pathdb%22%20AND%20a%3A%22pathdb%22)

A data store for graph paths. Take a look at our [website](http://www.pathdb.com/).

### What is PathDB?

PathDB, a data store for path shaped data. Use PathDB to super-charge your path-oriented graph queries. Built by a Graph Database expert, PathDB is an easy way to store and query your data. Primarliy used adjacent to fully-featured database as a graph-specific index.

### A simple way to store and query your data

The power behind PathDB is the form your data gets stored. Connected data is stored with their connections. By knowing you will query the data, paths of connections are expanded on to improve query time. And by how PathDB works, you can choose the balance between query speed and storage space. With the cost of disk space rapidly declining, this cost-analysis become more in favor of this type of data storage technique.

### Based on extensive research

PathDB is the result of a significant amount of research of graph data and graph structured indexing data structures. Based on the research, using PathDB can improve the speed of path queries by 1000x! To find out how PathDB works on a deeper level, dive into the research yourself.

1. *Path Indexing for Efficient Path Query Processing in Graph Databases*, Max Sumrall, M.Sc. Thesis, Technical University of Eindhoven. [PDF](http://alexandria.tue.nl/extra1/afstversl/wsk-i/Sumrall_2015.pdf)

2. *Investigations on path indexing for graph databases.*, Max Sumrall, George H. L. Fletcher, Alexandra Poulovassilis et al, Proc. PELGA 2016, in press, EuroPar 2016, Grenoble, France. [PDF](http://eprints.bbk.ac.uk/16329/7/16329.pdf)

### People

PathDB is the work of Max Sumrall. [[Github]](https://github.com/maxsumrall) [[LinkedIn]](http://www.linkedin.com/in/maxsumrall)


<!-- Old readme which described classes of the implementation before the refactoring -->
<!-- #### Current implementation

PathDB is a _k-path_ index implemented as a B+ tree. Datasets can be bulk-loaded into the index. The index can be sorted and paths can be found and merged into new entries of the index. The index and operations are disk-based and cached into memory. _IndexTree_ is the main class which handles most of the operations.

#### Goal implementation

PathDB consists out of __B+ tree implementation__ for the _k-path_ index. The B+ tree has operations for inserting, deleting and selecting. Query plans are interpreted by the __Interpreter__ module and this module is able to execute query plans by extending the B+ tree operations with merge-join and hash-join. PathDB is able to handle large files by reading and writing to disk. The module __DiskManager__ handles the reading and writing to disk.

B+ tree implementation
-----

This section describes each class of our B+ tree implementation.

### CompressedPageCursor

This class extends the _PageProxyCursor_ abstract class which implements the _Closeable_ class from _java<i></i>.io_.

This class describes the cursor which can be used for compressed pages. The following public methods are available:
- next
- pushChangesToDisk
- compress
- encodeKey
- numberOfBytes
- toBytes
- toLong
- getCurrentPageId
- capacity
- getSize
- setOffset
- getOffset
- getBytes
- getByte
- putBytes
- putByte
- getLong
- putLong
- getInt
- putInt
- leafNodeContainsSpaceForNewKey
- deferWriting
- resumeWriting
- internalNodeContainsSpaceForNewKeyAndChild
- close

### DiskCache

Class responsible for caching pages from disk by using the libraries _DefaultFileSystemAbstraction, PagedFile, SingleFilePageSwapperFactory, MuninnPageCache_ and _PageCacheTracer_ libraries from __Neo4j__.

### DiskCompressor

Class responsible for compressing a _DiskCache_.

### IndexBulkLoader

Build an _IndexTree_ from a given _DiskCache_. It has a public method _run_ to generate the B+ tree. It has one other public method to return the first key from a leaf: _traverseToFindFirstKeyInLeafAsBytes_. This last method uses the _getFirstKeyInNodeAsBytes_ method from the _IndexInsertion_ class.

It uses the following private methods to build the B+ tree:
- addLeafToParent
- buildUpperLeaves
- copyUpLeafToParent

### IndexDeletion

This class has methods to delete elements from the index.

The following public methods are available in an _IndexDeletion_:
- remove
- handleRemovedChildren
- removeKeyAndChildFromInternalNode
- removeKeyFromLeafNode
- removeChildAtIndex

Private methods:
- removeKeyAtOffset
- removeKeyAtIndex

### IndexInsertion

This class has methods to insert elements into the index.

The following public methods are available in an _IndexInsertion_:
- insert
- addKeyAndChildToInternalNode
- addKeyToLeafNode
- getFirstKeyInNode
- getFirstKeyInNodeAsBytes
- popFirstKeyInNodeAsBytes

Private methods:
- insertAndBalanceKeysBetweenLeafNodes
- insertAndBalanceKeysBetweenInternalNodes
- newKeyBelongsInNewNode
- insertKeyAtIndex
- insertChildAtIndex

### IndexSearch

This class contains methods to find elements in the index given a key as input. There are methods to return the result as an _int[]_ or as a _SearchCursor_. Public methods:
- find
- findWithCursor
- search

### IndexTree

This is the __main class__ which has class attributes instantiated from the classes _DiskCache, KeyImpl, IndexSearch, IndexInsertion and IndexDeletion_.

The following public methods are available in an instance of _IndexTree_:
- newRoot
- find
- insert
- remove
- setPrecedingId
- setFollowingId
- getChildIdAtIndex
- getIndexOfChild
- updateSiblingAndFollowingIdsInsertion
- updateSiblingAndFollowingIdsDeletion
- acquireNewLeafNode
- acquireNewInternalNode
- releaseNode
- removeFirstKeyInInternalNode

### InMemoryNode

Class to convert nodes from their compressed state to their uncompressed state. Note: nodes are saved to disk in their compressed state.

### KeyImpl

Class with the definition of the indexable form of a path, e.g. its key.

### NodeHeader

_final_ class containing the specification for the header of a node.

### NodeSize

Class to check if a node fits into a page.

### RemoveResultProxy

This is a class used by the _IndexDeletion_ class. A _RemoveResultProxy_ instance is created when a node will be deleted from the index. To handle the updates of siblings and children in the process of deletion, necessary information, e.g. _removedNodeId, siblingNodeID, isLeaf_, is stored in the _RemoveResultProxy_ instance.

### SearchCursor

Class to define a cursor for search operations on the B+ tree. This class has public functions _next()_ and _hasNext()_.

### SplitResult

The same way as the class _RemoveResultProxy_ is used for deletion, _SplitResult_ is used for insertion. The attributes _key, primkey, left and right_ are stored in an instance of _SplitResult_.

### TreeNodeIDManager

This class manages which ids a node of the tree can obtain. The following public methods are available to manage these ids:
- acquire
- release
- isNodeIdInFreePool

DiskManager
-----

_WORK IN PROGRESS_

This section describes how our limited memory size is used in combination with disk space to handle operations.

This module currently uses the _DefaultFileSystemAbstraction, PagedFile, SingleFilePageSwapperFactory, MuninnPageCache_ and _PageCacheTracer_ libraries from __Neo4j__.

Interpreter
-----

_WORK IN PROGRESS_

This section describes how query-plans are executed and where join-algorithms fit in. -->
