package bptree;

import org.junit.Test;
import org.junit.Ignore;

import java.io.FileNotFoundException;
import java.util.Random;


public class BPTreeTest{

    BPTree db;
    Random rnd = new Random();
    int numberOfKeys = 15;

    @Test
    public void logicTest(){
        Key a = new Key(new long[] {3, 4, 5, 6});
        Key b = new Key(new long[] {2, 1, 0, 0});
        Key c = new Key(new long[] {2, 2, 0, 0});

        assert(a.compareTo(b) == 1);
        assert(b.compareTo(a) == -1);
        assert(a.compareTo(a) == 0);
        assert(b.compareTo(c) == -1);
    }

    @Ignore
    public void sequentialTest() throws FileNotFoundException {
        /*
        Insert a bunch of stuff,
        then ask for it back,
        and make sure it's what we put in originally.
         */
        db = new BPTree();
        for (int i = 0; i < numberOfKeys; i++) {
            //System.out.println("Insert: " + i);
            db.insert(new Key(new long[]{i, i, i}));
        }
        //printGraph(db.bm.rootBlock);
        System.out.println("Find");
        for (int i = 0; i < numberOfKeys; i++) {
            //System.out.print("Find: " + i);
            //System.out.println("Found: " + db.find(new Key(new long[]{i,i,i}))[0].vals[0]);
            assert (db.find(new Key(new long[]{i, i, i}))[0].vals[0] == i);
        }


    }


    @Test
        public void randomTest() throws FileNotFoundException {
        /*
        Insert a bunch of random stuff,
        then ask for it back,
        and make sure it's what we put in originally.
         */
        long[][] testKeys = new long[numberOfKeys][3];
            db = new BPTree();
            for (int i = 0; i < numberOfKeys; i++) {
                testKeys[i][0] = rnd.nextInt(numberOfKeys);
                testKeys[i][1] = rnd.nextInt(numberOfKeys);
                testKeys[i][2] = rnd.nextInt(numberOfKeys);
                db.insert(new Key(new long[]{testKeys[i][0], testKeys[i][1], testKeys[i][2]}));
            }




        //shuffle test keys
            /*for(int i = testKeys.length - 1; i > 0; i--){
                int index = rnd.nextInt(i + 1);
                long[] a = testKeys[index];
                testKeys[index] = testKeys[i];
               testKeys[i] = a;
               */
            /*
            //try to find everything
            System.out.println("Find");
            for(int i = 0; i < numberOfKeys; i++){
                Key[] result = db.find(new Key(new long[]{testKeys[i][0], testKeys[i][1], testKeys[i][2]}));
                //System.out.println(result.length);
                assert(result.length == 1);
                try {
                    Key firstResult = result[0];
                    assert(firstResult.vals[0] == testKeys[i][0]);
                    assert(firstResult.vals[1] == testKeys[i][1]);
                    assert(firstResult.vals[2] == testKeys[i][2]);
                }
                catch (ArrayIndexOutOfBoundsException e){
                    bruteForceSearch(db, new Key(new long[]{testKeys[i][0], testKeys[i][1], testKeys[i][2]}));
                }
            }

            */

    }
    public void bruteForceSearch(BPTree db, Key key){
        System.out.println("Couldn't find Key: " + key.vals[0] + " " + key.vals[1] + " " +key.vals[2]);
        for (Block block : db.bm.blocks.values())
        {
            if(block instanceof LBlock){
                boolean notThisKey = false;
                for(Key lk : block.keys){
                    if(lk != null) {
                        if (lk.vals[0] != key.vals[0] || lk.vals[1] != key.vals[1] || lk.vals[2] != key.vals[2]) {
                            notThisKey = true;
                        }
                        if (!notThisKey) {
                            System.out.println("Value in Tree: " + lk.vals[0] + " " + lk.vals[1] + " " + lk.vals[2]);
                            return;
                        }
                    }

                }
            }

        }
    }

}