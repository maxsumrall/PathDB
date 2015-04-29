package bptree.impl;

import java.util.HashMap;

/**
 * Created by max on 4/29/15.
 */
public class LabelPathMapper {
    private HashMap<Long[], Long> labelPathMapping;
    private long nextPathId = 0;

    public LabelPathMapper(){
        labelPathMapping = new HashMap<>();
    }

    public long put(Long[] newLabelPath){
        labelPathMapping.put(newLabelPath, nextPathId++);
        return nextPathId - 1;
    }

    public long get(Long[] labelPath){
        Long pathId = labelPathMapping.get(labelPath);
        if(pathId != null){
            return pathId;
        }
        return put(labelPath);
    }

}
