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
public class KeyValueStorageRockDBTest {
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                //{dbConfigType, keyToCheck, startKey, endKey, bufferSize}
                {KeyValueStorageFactory.DbConfigType.Small, 0, 1, 2, SMALL_SIZE},
                {KeyValueStorageFactory.DbConfigType.Huge, 1, 1, 2, HUGE_SIZE},
                {KeyValueStorageFactory.DbConfigType.Small, 2, 1, 2, SMALL_SIZE},
                {KeyValueStorageFactory.DbConfigType.Small, 3, 1, 2, HUGE_SIZE},
                {KeyValueStorageFactory.DbConfigType.Small, 1, 1, 1, SMALL_SIZE},
        });
    }

    private static final String DIR = "/tmp/bookkeeper/testing";
    private KeyValueStorage dataStore;
    private final int keyToCheck;
    private final int startKey;
    private final int endKey;
    private final KeyValueStorageFactory.DbConfigType dbConfigType;
    private final int bufferSize;
    private static final int HUGE_SIZE = 64 * 1024 * 1024;
    private static final int SMALL_SIZE = 4;

    public KeyValueStorageRockDBTest(KeyValueStorageFactory.DbConfigType dbConfigType, int keyToCheck, int startKey, int endKey, int bufferSize) {
        this.keyToCheck = keyToCheck;
        this.startKey = startKey;
        this.endKey = endKey;
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
        for (int i = 0; i < this.endKey; i++) {
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
        for (int i = 0; i < this.endKey; i++) {
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
        first = ByteBuffer.allocate(bufferSize).putInt(this.startKey).array();
        //case empty db to improve condition coverage
        assertNull(dataStore.getFloor(first));

        dataStore.put(first, first);
        second = ByteBuffer.allocate(bufferSize).putInt(this.endKey).array();
        dataStore.put(second, second);
        toCheck = ByteBuffer.allocate(bufferSize).putInt(this.keyToCheck).array();
        if (this.keyToCheck > endKey) {
            check = dataStore.getFloor(toCheck).getKey();
            assertArrayEquals(check, second);
        } else if (this.keyToCheck > startKey) {
            check = dataStore.getFloor(toCheck).getKey();
            assertArrayEquals(check, first);
        } else {
            assertNull(dataStore.getFloor(toCheck));
        }
    }

    @Test
    public void getCeilTest() throws IOException {
        byte[] first, second, check, toCheck;
        //simple insert two arbitrary values and check ceil output
        first = ByteBuffer.allocate(bufferSize).putInt(this.startKey).array();
        dataStore.put(first, first);
        second = ByteBuffer.allocate(bufferSize).putInt(this.endKey).array();
        dataStore.put(second, second);
        toCheck = ByteBuffer.allocate(bufferSize).putInt(this.keyToCheck).array();


        if (keyToCheck > startKey && keyToCheck <= this.endKey) {
            check = dataStore.getCeil(toCheck).getKey();
            assertArrayEquals(check, second);
        } else if (keyToCheck <= startKey) {
            check = dataStore.getCeil(toCheck).getKey();
            assertArrayEquals(check, first);
        } else {
            assertNull(dataStore.getCeil(toCheck));
        }
    }

    @Test
    public void keysSpanningTest() throws IOException {
        byte[] current, last, check, b;
        //populate db
        for (int i = startKey; i < endKey; i++) {
            //before insert
            b = ByteBuffer.allocate(bufferSize).putInt(i).array();
            check = this.dataStore.get(b);
            assertNull(check);

            //insert value
            this.dataStore.put(b, b);
            check = this.dataStore.get(b);
            assertNotNull(check);

        }

        assertEquals(endKey - startKey, dataStore.count());
        int counter = this.startKey;
        current = ByteBuffer.allocate(bufferSize).putInt(counter).array();
        last = ByteBuffer.allocate(bufferSize).putInt(endKey).array();

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
        int res;
        //populate db
        for (int i = this.startKey; i <= this.endKey; i++) {
            b = ByteBuffer.allocate(bufferSize).putInt(i).array();
            check = new byte[bufferSize];
            //insert value
            this.dataStore.put(b, b);
            // check insert success
            res = this.dataStore.get(b, check);
            assertNotEquals(-1, res);
            dataStore.delete(b);
            //check remove success
            res = this.dataStore.get(b, check);
            //case NOT_FOUND
            assertEquals(-1, res);

        }

        b = ByteBuffer.allocate(bufferSize).putInt(this.startKey).array();
        //insert value
        dataStore.put(b, b);
        //use ad-hoc small array improve statement
        check = new byte[bufferSize - 1];
        // trigger exception
        dataStore.get(b, check);

    }



    @Test
    public void deleteRangeTest() throws IOException {
        byte[] beginKey, endKey;
        long sizeBefore = dataStore.count();
        //datastore is empty
        assertEquals(0, sizeBefore);
        beginKey = ByteBuffer.allocate(bufferSize).putInt(startKey).array();
        endKey = ByteBuffer.allocate(bufferSize).putInt(this.endKey).array();
        KeyValueStorage.Batch batch = dataStore.newBatch();
        if (this.startKey != this.endKey){
            // insert values to the datastore
            dataStore.put(beginKey, beginKey);
            dataStore.put(endKey, endKey);
            assertEquals(2, dataStore.count());
        }
        endKey = ByteBuffer.allocate(bufferSize).putInt(this.endKey + 1).array();
        batch.deleteRange(beginKey, endKey);
        batch.flush();
        batch.close();

        if (this.startKey == this.endKey){
            //db should be empty
            assertEquals(0, dataStore.count());// <----- failure deleteRange should do nothing here; why a put operation is performed ?!
            return;
        }

        assertNull(dataStore.get(beginKey));
        assertNull(dataStore.get(endKey));


    }

}
