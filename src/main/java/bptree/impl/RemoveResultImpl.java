package bptree.impl;

import bptree.RemoveResult;

public class RemoveResultImpl implements RemoveResult{

    private int removedDocuments;

    public RemoveResultImpl(int removedDocuments){
        this.removedDocuments = removedDocuments;
    }

    public int getN(){
        return removedDocuments;
    }
}
