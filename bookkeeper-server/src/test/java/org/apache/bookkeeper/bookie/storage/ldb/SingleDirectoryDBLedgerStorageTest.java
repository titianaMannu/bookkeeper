package org.apache.bookkeeper.bookie.storage.ldb;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.EntryLocation;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class SingleDirectoryDBLedgerStorageTest {

    public SingleDirectoryDBLedgerStorageTest(InputTest input) {
        this.ledgerId = input.getLedgerId();
        this.entryId1 = input.getEntryId1();
        this.entryId2 = input.getEntryId2();
        this.content = input.getContent();
        this.masterKey = input.getMasterKey();
        this.poolSize = input.getPoolSize();
    }

    @Parameterized.Parameters
    public static Collection<InputTest> data() {
        List<InputTest> list = new ArrayList<>();

        /*ledgerId, entryId1, entryId2, content, masterKey, threadPoolSize*/
        list.add(new InputTest(1, 0, 1, "content", "key", 1));
        list.add(new InputTest(0, 1, 2, "", "", 200));
        list.add(new InputTest(-1, -1, 1, "content", "key", 1));
        list.add(new InputTest(1, 1, 1, "content", "key", 1));
        return list;

    }

    private static final String DIR = "/tmp/bookkeeper/testing";
    private SingleDirectoryDbLedgerStorage storage;
    private File dir;
    private final long ledgerId;
    private final long entryId1;
    private final long entryId2;
    private final String content;
    private final String masterKey;
    private ExecutorService pool;
    private final int poolSize;

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "trace");
        Files.createDirectories(Paths.get(DIR));
    }

    @Before
    public void setUp() throws Exception {

        pool = Executors.newFixedThreadPool(poolSize);

        this.dir = new File(DIR);
        FileUtils.cleanDirectory(this.dir);

        File directory = Bookie.getCurrentDirectory(this.dir);
        Bookie.checkDirectoryStructure(directory);

        int gcTime = 1000;
        ServerConfiguration configuration = TestBKConfiguration.newServerConfiguration();
        configuration.setGcWaitTime(gcTime);
        configuration.setLedgerStorageClass(DbLedgerStorage.class.getName());
        configuration.setLedgerDirNames(new String[]{this.dir.toString()});
        Bookie bookie = new Bookie(configuration);

        storage = ((DbLedgerStorage) bookie.getLedgerStorage()).getLedgerStorageList().get(0);


    }

    @After
    public void tearDown() throws Exception {
        storage.shutdown();
        this.dir.delete();
    }

    @Test
    public void basicAddAndUpdate() throws Exception {
        Assume.assumeTrue(ledgerId >= 0 && entryId1 >= 0 && entryId2 >= 0);
        storage.setMasterKey(ledgerId, masterKey.getBytes());
        ByteBuf buf2, buf1, buf3;

        //buffer1 initialization
        buf1 = initializeBuffer(ledgerId, entryId1, content);
        storage.addEntry(buf1);
        ByteBuf resBuf = storage.getEntry(ledgerId, entryId1);
        buf1 = storage.getLastEntry(ledgerId);
        assertEquals("should find:" + buf1.toString(), buf1, resBuf);

        //buffer2 initialization
        buf2 = initializeBuffer(ledgerId, entryId2, content);
        storage.addEntry(buf2);
        resBuf = storage.getEntry(ledgerId, entryId2);
        assertEquals("should find:" + buf2, buf2, resBuf);

        if (entryId1 == entryId2) {
            // they are simply the same
            assertEquals(storage.getEntry(ledgerId, entryId1), storage.getEntry(ledgerId, BookieProtocol.LAST_ADD_CONFIRMED));
        }

        // same entryId of buf1 but different content
        buf3 = initializeBuffer(ledgerId, entryId1, content + ":" + content);
        //add using the logger
        long newLocationId = storage.getEntryLogger().addEntry(ledgerId, buf3, false);
        // output of get should be buf1
        assertEquals("should find:" + buf1, buf1, storage.getEntry(ledgerId, entryId1));

        EntryLocation newLocation = new EntryLocation(ledgerId, entryId1, newLocationId);
        // from now on entryId1 will point to the new location corresponding to buf3
        storage.updateEntriesLocations(Lists.newArrayList(newLocation));
        storage.flushEntriesLocationsIndex();
        resBuf = storage.getEntry(ledgerId, entryId1);
        assertEquals("should find:" + buf3, buf3, resBuf);

        if (entryId1 == entryId2) {
            // they were the same before, so I should see the update
            assertEquals("should find:" + buf3, resBuf, storage.getEntry(ledgerId, entryId2));
        } else {
            //buf2 remains the same
            resBuf = storage.getEntry(ledgerId, entryId2);
            assertEquals("should find:" + buf2, buf2, resBuf);
        }

    }

    private ByteBuf initializeBuffer(long ledgerId, long entryId, String content) {
        ByteBuf buf = Unpooled.buffer(1024);
        buf.writeLong(ledgerId);
        buf.writeLong(entryId);
        buf.writeBytes(content.getBytes());
        return buf;
    }

    @Test(expected = Exception.class)
    public void testIllegalKey() throws IOException, BookieException {
        Assume.assumeTrue(ledgerId < 0);
        try {
            ByteBuf buf = initializeBuffer(ledgerId, entryId1, content);
            storage.setMasterKey(ledgerId, masterKey.getBytes());
            storage.addEntry(buf);
        } catch (IOException | BookieException e) {
            e.printStackTrace();
            throw e;
        }

    }

    @Test
    public void addEntriesConcurrently() throws IOException, InterruptedException {
        Assume.assumeTrue(ledgerId >= 0 && entryId2 >= 0 && entryId1 >= 0);
        storage.setMasterKey(ledgerId, masterKey.getBytes());
        for (int i = 0; i < poolSize; i++) {
            final int j = i;
            pool.execute(() -> {
                //buffer1 initialization
                ByteBuf buf1 = initializeBuffer(ledgerId, j, content);
                try {
                    storage.setMasterKey(ledgerId, masterKey.getBytes());
                    storage.addEntry(buf1);
                    //cache rotation
                    storage.flush();

                    storage.flushEntriesLocationsIndex();
                    ByteBuf resBuf = storage.getEntry(ledgerId, j);

                    assertEquals("should find:" + buf1, buf1, resBuf);

                    storage.deleteLedger(ledgerId);
                    storage.flush();

                } catch (IOException | BookieException e) {
                    e.printStackTrace();
                }
            });
        }

        pool.shutdown();
        if (pool.awaitTermination(1, TimeUnit.MINUTES)) {
            System.out.println("entries correctly added.");
        }
    }

    @Test(expected = Bookie.NoEntryException.class)
    public void getInvalidEntry() throws IOException {
        Assume.assumeTrue(ledgerId >= 0);
        storage.setMasterKey(ledgerId, masterKey.getBytes());
        storage.getEntry(ledgerId, entryId1);
    }

    private static class InputTest {
        private final long ledgerId;
        private final long entryId1;
        private final long entryId2;
        private final String content;
        private final String masterKey;
        private final int poolSize;

        public InputTest(long ledgerId, long entryId1, long entryId2, String content, String masterKey, int poolSize) {
            this.ledgerId = ledgerId;
            this.entryId1 = entryId1;
            this.entryId2 = entryId2;
            this.content = content;
            this.masterKey = masterKey;
            this.poolSize = poolSize;
        }

        public long getLedgerId() {
            return ledgerId;
        }

        public long getEntryId1() {
            return entryId1;
        }

        public long getEntryId2() {
            return entryId2;
        }

        public String getContent() {
            return content;
        }

        public String getMasterKey() {
            return masterKey;
        }

        public int getPoolSize() {
            return poolSize;
        }
    }

}
