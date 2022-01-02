package org.apache.bookkeeper.bookie.storage.ldb;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class MyKVStorageTest {
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {KeyValueStorageFactory.DbConfigType.Small, 5, 1, 5, 4},
                {KeyValueStorageFactory.DbConfigType.Huge,  1, 1, 5, HUGE_SIZE},
                {KeyValueStorageFactory.DbConfigType.Small,  6, 1, 5, HUGE_SIZE},
                {KeyValueStorageFactory.DbConfigType.Small,  1, 2, 5, 4},
        });
    }

    private static final String DIR = "/tmp/bookkeeper/testing";
    private KeyValueStorage dataStore;
    private final int keyToCheck;
    private final int start;
    private final int end;
    private final KeyValueStorageFactory.DbConfigType dbConfigType;
    private final int bufferSize;
    private static final int HUGE_SIZE = 64 * 1024 * 1024;

    public MyKVStorageTest( KeyValueStorageFactory.DbConfigType dbConfigType, int k, int start, int end, int bufferSize) {
        this.keyToCheck = k;
        this.start = start;
        this.end = end;
        this.dbConfigType = dbConfigType;
        this.bufferSize = bufferSize;
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        Files.createDirectories(Paths.get(DIR));
    }

    @Before
    public void setUp() throws Exception {
        ServerConfiguration configuration = new ServerConfiguration();

        File f = new File(DIR);
        FileUtils.cleanDirectory(f);
        this.dataStore = new KeyValueStorageRocksDB(DIR, this.dbConfigType, configuration, false);
    }

    @After
    public void tearDown() throws Exception {
        this.dataStore.close();
    }

    @Test
    public void basicPutGetRemoveTest() throws Exception {
        // empty store at the beginning
        assertEquals(0, this.dataStore.count());

        byte[] b;
        byte[] check;
        //populate db
        for (int i = 0; i < this.end; i++) {
            //before insert
            b = ByteBuffer.allocate(bufferSize).putInt(i).array();
            check = this.dataStore.get(b);
            assertNull(check);

            //insert value
            this.dataStore.put(b, b);
            check = this.dataStore.get(b);
            assertNotNull(check);

        }

        KeyValueStorage.Batch batch = dataStore.newBatch();
        for (int i = 0; i < this.end; i++) {
            b = ByteBuffer.allocate(bufferSize).putInt(i).array();
            batch.remove(b);
            //removal will take place atomically when batch.flush is called
            assertNotNull(this.dataStore.get(b));
        }
        batch.flush();
        assertEquals(0, this.dataStore.count());
        batch.close();

    }


    @Test
    public void getFloorTest() throws IOException {

        byte[] first;
        byte[] second;
        byte[] check;
        byte[] toCheck;
        //simple insert two arbitrary values and check floor output
        first = ByteBuffer.allocate(bufferSize).putInt(this.start).array();
        dataStore.put(first, first);
        second = ByteBuffer.allocate(bufferSize).putInt(this.end).array();
        dataStore.put(second, second);
        toCheck = ByteBuffer.allocate(bufferSize).putInt(this.keyToCheck).array();
        if ( this.keyToCheck > end) {
            check = dataStore.getFloor(toCheck).getKey();
            assertArrayEquals(check, second);
        } else if (this.keyToCheck > start) {
            check = dataStore.getFloor(toCheck).getKey();
            assertArrayEquals(check, first);
        }   else{
            assertNull(dataStore.getFloor(toCheck));
        }
    }

    @Test
    public void getCeilTest() throws IOException {
        byte[] first, second, check, toCheck;
        //simple insert two arbitrary values and check ceil output
        first = ByteBuffer.allocate(bufferSize).putInt(this.start).array();
        dataStore.put(first, first);
        second = ByteBuffer.allocate(bufferSize).putInt(this.end).array();
        dataStore.put(second, second);
        toCheck = ByteBuffer.allocate(bufferSize).putInt(this.keyToCheck).array();


        if (keyToCheck > start && keyToCheck <= this.end) {
            check = dataStore.getCeil(toCheck).getKey();
            assertArrayEquals(check, second);
        } else if (keyToCheck <= start){
            check = dataStore.getCeil(toCheck).getKey();
            assertArrayEquals(check, first);
        }else {
            assertNull( dataStore.getCeil(toCheck));
        }
    }

    @Test
    public void keysSpanningTest() throws IOException {
        byte[] current, last, check, b;
        //populate db
        for (int i = start; i < end; i++) {
            //before insert
            b = ByteBuffer.allocate(bufferSize).putInt(i).array();
            check = this.dataStore.get(b);
            assertNull(check);

            //insert value
            this.dataStore.put(b, b);
            check = this.dataStore.get(b);
            assertNotNull(check);

        }

        assertEquals(end - start, dataStore.count());
        int counter = this.start;
        current = ByteBuffer.allocate(bufferSize).putInt(counter).array();
        last = ByteBuffer.allocate(bufferSize).putInt(end ).array();

        KeyValueStorage.CloseableIterator<byte[]> keyIterator = dataStore.keys(current, last);
        while (keyIterator.hasNext()) {
            current = ByteBuffer.allocate(bufferSize).putInt(counter).array();
            //retrieve the key
            check = keyIterator.next();
            assertArrayEquals(current, check);
            counter++;

        }
        keyIterator.close();
    }


    @Test(expected = IOException.class)
    public void insertGetRemoveRepeatedly() throws IOException {
        byte[] b;
        byte[] check;
        KeyValueStorage.Batch batch = dataStore.newBatch();
        int res;
        //populate db
        for (int i = this.start;  i <= this.end; i++) {
            b = ByteBuffer.allocate(bufferSize).putInt(i).array();
            //insert value
            this.dataStore.put(b, b);
            check = new byte[bufferSize];
            // check insert success
            res = this.dataStore.get(b, check);
            assertNotEquals(-1, res);

            batch.remove(b);
            batch.flush();
            //check remove success
            res = this.dataStore.get(b, check);
            assertEquals(-1, res);

        }
        batch.close();

        b = ByteBuffer.allocate(bufferSize).putInt(this.start).array();
        //insert value
        dataStore.put(b, b);
        //use ad-hoc small array
        check = new byte[bufferSize - 1];
        // trigger exception
        dataStore.get(b, check);

    }


    public static class UnexpectedBehaviorException extends Exception {
        public UnexpectedBehaviorException(String errorMessage) {
            super(errorMessage);
            System.out.println(errorMessage);
        }
    }

    @Test(expected = UnexpectedBehaviorException.class)
    public void deleteRangeTest() throws IOException, UnexpectedBehaviorException {
        byte[] beginKey, endKey;
        long sizeBefore = dataStore.count();

        KeyValueStorage.Batch batch = dataStore.newBatch();
        beginKey = ByteBuffer.allocate(bufferSize).putInt(start).array();
        endKey = ByteBuffer.allocate(bufferSize).putInt(end).array();
        batch.deleteRange(beginKey, endKey); // <----- failure deleteRange should do nothing here; why a put operation is performed ?!
        batch.flush();
        batch.close();
        long sizeAfter = dataStore.count();
        if ( sizeAfter != sizeBefore){
            throw new UnexpectedBehaviorException("deleteRangeTest: The datastore should be empty.");
        }
    }

}
