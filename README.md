PathDB
=====

[![Build Status](https://travis-ci.org/maxsumrall/PathDB.svg?branch=master)](https://travis-ci.org/maxsumrall/PathDB)

A data store for graph paths.

#### Current implementation

PathDB is a _k-path_ index implemented as a B+ tree. Datasets can be bulk-loaded into the index. The index can be sorted and paths can be found and merged into new entries of the index. The index and operations are disk-based and cached into memory. _IndexTree_ is the main class which handles most of the operations.

#### Goal implementation

PathDB consists out of __B+ tree implementation__ for the _k-path_ index. The B+ tree has operations for inserting, deleting and selecting. Query plans are interpreted by the __Interpreter__ module and this module is able to execute query plans by extending the B+ tree operations with merge-join and hash-join. PathDB is able to handle large files by reading and writing to disk. The module __DiskManager__ handles the reading and writing to disk.

B+ tree implementation
-----

This section describes each class of our B+ tree implementation.

### CompressedPageCursor



### DiskCache

Class responsible for caching pages from disk by using the libraries _DefaultFileSystemAbstraction, PagedFile, SingleFilePageSwapperFactory, MuninnPageCache_ and _PageCacheTracer_ libraries from __Neo4j__.

### DiskCompressor

Class responsible for compressing a _DiskCache_.

### IndexBulkLoader

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

### IndexTree

This is the __main class__ which has class attributes instantiated from the classes _DiskCache, KeyImpl, IndexSearch, IndexInsertion and IndexDeletion_.

The following public methods are available in an _IndexTree_:
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

### SearchCursor

Class to define a cursor for search operations on the B+ tree. This class has public functions _next()_ and _hasNext()_.

### SplitResult

### TreeNodeIDManager

DiskManager
-----

_WORK IN PROGRESS_

This section describes how our limited memory size is used in combination with disk space to handle operations.

This module currently uses the _DefaultFileSystemAbstraction, PagedFile, SingleFilePageSwapperFactory, MuninnPageCache_ and _PageCacheTracer_ libraries from __Neo4j__.

Interpreter
-----

_WORK IN PROGRESS_

This section describes how query-plans are executed and where join-algorithms fit in.
