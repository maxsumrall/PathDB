package bptree.impl;

import java.util.LinkedList;

/**
 * Created by max on 4/13/15.
 */
public class AvailablePageIdPool {
    private static LinkedList<Long> pool = null;
    private static long currentID = 0;
    //private long maximumNumberOfPages;

    public AvailablePageIdPool(long maximumNumberOfPages){
        if(pool == null) {
            pool = new LinkedList<>();
        }
    }

    public static Long acquireId(){
        if(pool == null) {
            pool = new LinkedList<>();
        }
        if(pool.size() > 0) {
            return pool.pop();
        }
        else{
            return currentID++;
        }
    }

    public static void releaseId(Long id){
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
