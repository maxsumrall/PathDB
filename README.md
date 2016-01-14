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

This section describes how query-plans are executed and where join-algorithms fit in.
