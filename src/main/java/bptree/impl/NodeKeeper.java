package bptree.impl;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

public class NodeKeeper {
    protected DiskCache diskCache;
    int max_cache_size_mb = 1024;
    private int max_cache_size;
    private LRUCache<Long, Node> cache;
    private Tree tree;
    public NodeKeeper(Tree tree, DiskCache diskCache){
        this.tree = tree;
        this.diskCache = diskCache;
        max_cache_size = (max_cache_size_mb * 1000000) / diskCache.pageCachePageSize; //TODO this is a tunable parameter
        cache = new LRUCache<>(max_cache_size);
    }

    public Node getNode(long id) throws IOException {
        Node node = cache.get(id);
        if(node != null){
            return node;
        }
        else {
            ByteBuffer buffer = this.diskCache.readPage(id);
            if (buffer.capacity() == 0) {
                throw new IOException("Unable to read page from cache. Page: " + id);
            }
            if (NodeHeader.isLeafNode(buffer)) {
                node = LeafNode.instantiateNodeFromBuffer(buffer, tree, id);
            } else {
                node = InternalNode.instantiateNodeFromBuffer(buffer, tree, id);
            }
            cache.put(node.id, node);
            return node;
        }
    }

    public void writeNodeToPage(Node node){
        this.diskCache.writePage(node.id, node.serialize().array());
        cache.put(node.id, node);
    }

    public void shutdown() throws IOException {
        for(Node node : cache.values()){
            diskCache.writePage(node.id, node.serialize().array());
        }
        diskCache.shutdown();
    }

    private class LRUCache<Long, Node> extends LinkedHashMap<Long, Node> {
        private int cacheSize;

        public LRUCache(int cacheSize) {
            super(cacheSize, 0.75f, true);
            this.cacheSize = cacheSize;
        }

    protected boolean removeEldestEntry(Map.Entry<Long, Node> eldest) {
        return this.size() >= cacheSize;
    }
}


}
