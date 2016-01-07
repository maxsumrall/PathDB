/**
 * Copyright (C) 2015-2016 - All rights reserved.
 * This file is part of the PathDB project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package NeoIntegration;

import java.io.IOException;

public class Runner {
    public static void main(String[] args) throws IOException {
        if(args[0].equals("1")){
            BulkLUBMDataLoader.main(args);
        }
        else if (args[0].equals("2")){
            CleverIndexBuilder.main(args);
        }
        else if (args[0].equals("3")){
            LexicographicIndexBuilder.main(args);
        }
        else if (args[0].equals("5")){
            LDBCBenchmarkExperiment.main(args);
        }
    }
}
