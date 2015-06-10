package bptree.impl;


import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

public class CompressedPageCache {
    int max_cache_size_mb = 2048;
    private int max_cache_size;
    private LRUCache<Long, ByteBuffer> cache;
    public CompressedPageCache(){
        max_cache_size = (max_cache_size_mb * 1000000) / DiskCache.PAGE_SIZE; //TODO this is a tunable parameter
        cache = new LRUCache<>(max_cache_size);
    }

    public ByteBuffer getByteBuffer(long id)  {
        return cache.get(id);
    }

    public void putByteBuffer(long pageId, ByteBuffer buffer) {
        cache.put(pageId, buffer);
    }



    private class LRUCache<Long, ByteBuffer> extends LinkedHashMap<Long, ByteBuffer> {
        private int cacheSize;

        public LRUCache(int cacheSize) {
            super(cacheSize, 0.75f, true);
            this.cacheSize = cacheSize;
        }
        protected boolean removeEldestEntry(Map.Entry<Long, ByteBuffer> eldest) {
            return this.size() >= cacheSize;
        }
    }


}
