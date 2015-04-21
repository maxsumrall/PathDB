package bptree.impl;

import java.util.LinkedList;

/**
 * Created by max on 4/13/15.
 */
public class AvailablePageIdPool {
    private LinkedList<Long> pool;

    public AvailablePageIdPool(long maximumNumberOfPages){
        pool = new LinkedList<>();
        for(long i = 0; i < maximumNumberOfPages; i++){
            pool.add(i);
        }
    }

    public Long acquireId(){
        return pool.pop();
    }

    public void releaseId(Long id){
        pool.push(id);
    }
}
