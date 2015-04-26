package bptree.impl;

import java.util.LinkedList;

/**
 * Created by max on 4/13/15.
 */
public class AvailablePageIdPool {
    private LinkedList<Long> pool;
    private long currentID = 0;
    //private long maximumNumberOfPages;

    public AvailablePageIdPool(long maximumNumberOfPages){
        pool = new LinkedList<>();
        //this.maximumNumberOfPages = maximumNumberOfPages;
    }

    public Long acquireId(){
        if(pool.size() > 0) {
            return pool.pop();
        }
        else{
            return currentID++;
        }
    }

    public void releaseId(Long id){
        pool.push(id);
    }

    public boolean isNodeIdInFreePool(Long search_id){
        for(Long id : pool){
            if(search_id.equals(id)){
                return true;
            }
        }
        return false;
    }
}
