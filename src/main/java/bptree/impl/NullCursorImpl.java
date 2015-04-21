package bptree.impl;

import bptree.Cursor;

public class NullCursorImpl implements Cursor {

    @Override
    public Long[] next() {
        return new Long[]{};
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public LeafNode getCurrentLeaf(){
        return null;
    }
}
