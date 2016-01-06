/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathIndex project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package bptree.impl;

import java.util.HashMap;

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
