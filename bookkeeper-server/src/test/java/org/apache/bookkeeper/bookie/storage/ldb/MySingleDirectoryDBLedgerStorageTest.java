package org.apache.bookkeeper.bookie.storage.ldb;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.EntryLocation;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
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

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class MySingleDirectoryDBLedgerStorageTest {

    public MySingleDirectoryDBLedgerStorageTest(InputTest input) {
        this.ledgerId = input.getLedgerId();
        this.entryId1 = input.getEntryId1();
        this.entryId2 = input.getEntryId2();
        this.content = input.getContent();
        this.masterKey = input.getMasterKey();
    }

    @Parameterized.Parameters
    public static Collection<InputTest> data() {
        List<InputTest> list = new ArrayList<>();

        list.add(new InputTest(1, 1, 2, "content", "key"));
        list.add(new InputTest(0, 1, 2, "", ""));
        list.add(new InputTest(-1, 1, 1, "content", "key"));
        list.add(new InputTest(1, 2, 2, "content", ""));

        return list;

    }

    private static final String DIR = "/tmp/bookkeeper/testing";
    private SingleDirectoryDbLedgerStorage storage;
    private File dir;
    private final int ledgerId;
    private final int entryId1;
    private final int entryId2;
    private final String content;
    private final String masterKey;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Files.createDirectories(Paths.get(DIR));
    }

    @Before
    public void setUp() throws Exception {

        this.dir = new File(DIR);
        FileUtils.cleanDirectory(this.dir);

        File curDir = Bookie.getCurrentDirectory(this.dir);
        Bookie.checkDirectoryStructure(curDir);

        int gcWaitTime = 1000;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setGcWaitTime(gcWaitTime);
        conf.setLedgerStorageClass(DbLedgerStorage.class.getName());
        conf.setLedgerDirNames(new String[] { this.dir.toString() });
        Bookie bookie = new Bookie(conf);

        storage = ( (DbLedgerStorage) bookie.getLedgerStorage()).getLedgerStorageList().get(0);

    }

    @After
    public void tearDown() throws Exception {
        storage.shutdown();
        this.dir.delete();
    }

    @Test
    public void basicAddAndUpdate() throws Exception {
        Assume.assumeTrue(ledgerId >= 0);
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

        if (entryId1 == entryId2){
            // they are simply the same
            assertEquals(storage.getEntry(ledgerId, entryId1), resBuf);
        }

        // same entryId of buf1 but different content
        buf3 = initializeBuffer(ledgerId, entryId1, content + ":" + content);
        //add by using the logger
        long newLocationId = storage.getEntryLogger().addEntry(ledgerId, buf3, false);
        // output of get should be buf1
        assertEquals("should find:" + buf1, buf1, storage.getEntry(ledgerId, entryId1));


        EntryLocation newLocation = new EntryLocation(ledgerId, entryId1, newLocationId);
        // from now on entryId1 will point to the new location corresponding to buf3
        storage.updateEntriesLocations(Lists.newArrayList(newLocation));
        resBuf = storage.getEntry(ledgerId, entryId1);
        assertEquals("should find:" + buf3, buf3, resBuf);

        if (entryId1 == entryId2){
            // they were the same before, so I should see the update
            assertEquals("should find:" + buf3, resBuf, storage.getEntry(ledgerId, entryId2));
        }else{
            //buf2 remains the same
            resBuf = storage.getEntry(ledgerId, entryId2);
            assertEquals("should find:" + buf2, buf2, resBuf);
        }

    }

    private ByteBuf initializeBuffer(long ledgerId, long entryId, String content){
        ByteBuf buf =  Unpooled.buffer(1024);
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

   private static class InputTest{
       private final int ledgerId;
       private final int entryId1;
       private final int entryId2;
       private final String content;
       private final String masterKey;

       public InputTest(int ledgerId, int entryId1, int entryId2, String content, String masterKey) {
           this.ledgerId = ledgerId;
           this.entryId1 = entryId1;
           this.entryId2 = entryId2;
           this.content = content;
           this.masterKey = masterKey;
       }

       public int getLedgerId() {
           return ledgerId;
       }

       public int getEntryId1() {
           return entryId1;
       }

       public int getEntryId2() {
           return entryId2;
       }

       public String getContent() {
           return content;
       }

       public String getMasterKey() {
           return masterKey;
       }
   }

}
