/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fess.ds.dropbox;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.helper.PermissionHelper;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.opensearch.config.exentity.CrawlingConfig;
import org.codelibs.fess.opensearch.config.exentity.FailureUrl;
import org.codelibs.fess.util.ComponentUtil;
import org.codelibs.fess.ds.dropbox.UnitDsTestCase;

import com.dropbox.core.DbxException;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.FolderMetadata;
import com.dropbox.core.v2.files.Metadata;

public class DropboxDataStoreTest extends UnitDsTestCase {
    public DropboxDataStore dataStore;

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Override
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        dataStore = new DropboxDataStore();
    }

    @Override
    public void tearDown(TestInfo testInfo) throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown(testInfo);
    }

    public void test_getUrl() throws Exception {
        assertEquals("https://www.dropbox.com/home/Test%201/Test%202/fi%20le.png", dataStore.getUrl("/Test 1/Test 2/fi le.png"));
    }

    public void test_getUrl_simple() throws Exception {
        assertEquals("https://www.dropbox.com/home/test.txt", dataStore.getUrl("/test.txt"));
    }

    public void test_getUrl_withSpecialCharacters() throws Exception {
        assertEquals("https://www.dropbox.com/home/test%20&%20file.pdf", dataStore.getUrl("/test & file.pdf"));
    }

    public void test_getUrl_withJapanese() throws Exception {
        String result = dataStore.getUrl("/テスト/ファイル.txt");
        assertNotNull(result);
        assertTrue(result.startsWith("https://www.dropbox.com/home/"));
    }

    public void test_getUrl_nested() throws Exception {
        assertEquals("https://www.dropbox.com/home/folder1/folder2/folder3/file.docx",
                dataStore.getUrl("/folder1/folder2/folder3/file.docx"));
    }

    public void test_getFileMimeType_fromName() throws Exception {
        java.util.Date now = new java.util.Date();
        FileMetadata file = FileMetadata.newBuilder("test.txt", "id-1", now, now, "0123456789abcdef0123456789abcdef01234567", 100L).build();
        InputStream in = new ByteArrayInputStream(new byte[0]);
        String mimeType = dataStore.getFileMimeType(in, file);
        assertEquals("text/plain", mimeType);
    }

    public void test_getFileMimeType_pdf() throws Exception {
        java.util.Date now = new java.util.Date();
        FileMetadata file =
                FileMetadata.newBuilder("document.pdf", "id-2", now, now, "0123456789abcdef0123456789abcdef01234567", 100L).build();
        InputStream in = new ByteArrayInputStream(new byte[0]);
        String mimeType = dataStore.getFileMimeType(in, file);
        assertEquals("application/pdf", mimeType);
    }

    public void test_getFileMimeType_unknown() throws Exception {
        java.util.Date now = new java.util.Date();
        FileMetadata file =
                FileMetadata.newBuilder("file.unknown", "id-3", now, now, "0123456789abcdef0123456789abcdef01234567", 100L).build();
        InputStream in = new ByteArrayInputStream(new byte[0]);
        String mimeType = dataStore.getFileMimeType(in, file);
        assertEquals("application/octet-stream", mimeType);
    }

    public void test_getFileMimeType_fromStream() throws Exception {
        java.util.Date now = new java.util.Date();
        FileMetadata file = FileMetadata.newBuilder("test", "id-4", now, now, "0123456789abcdef0123456789abcdef01234567", 100L).build();
        // GIF header
        byte[] gifHeader = new byte[] { 0x47, 0x49, 0x46, 0x38, 0x39, 0x61 };
        InputStream in = new ByteArrayInputStream(gifHeader);
        String mimeType = dataStore.getFileMimeType(in, file);
        assertEquals("image/gif", mimeType);
    }

    public void test_getName() throws Exception {
        assertEquals("DropboxDataStore", dataStore.getName());
    }

    public void test_Config_defaults() throws Exception {
        DataStoreParams paramMap = new DataStoreParams();
        DropboxDataStore.Config config = new DropboxDataStore.Config(paramMap);
        assertEquals(10000000L, config.maxSize);
        assertTrue(config.ignoreFolder);
        assertTrue(config.ignoreError);
        assertEquals(1, config.supportedMimeTypes.length);
        assertEquals(".*", config.supportedMimeTypes[0]);
    }

    public void test_Config_customMaxSize() throws Exception {
        DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("max_size", "5000000");
        DropboxDataStore.Config config = new DropboxDataStore.Config(paramMap);
        assertEquals(5000000L, config.maxSize);
    }

    public void test_Config_invalidMaxSize() throws Exception {
        DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("max_size", "invalid");
        DropboxDataStore.Config config = new DropboxDataStore.Config(paramMap);
        assertEquals(10000000L, config.maxSize); // falls back to default
    }

    public void test_Config_ignoreFolder_false() throws Exception {
        DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_folder", "false");
        DropboxDataStore.Config config = new DropboxDataStore.Config(paramMap);
        assertFalse(config.ignoreFolder);
    }

    public void test_Config_ignoreError_false() throws Exception {
        DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_error", "false");
        DropboxDataStore.Config config = new DropboxDataStore.Config(paramMap);
        assertFalse(config.ignoreError);
    }

    public void test_Config_supportedMimeTypes() throws Exception {
        DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("supported_mimetypes", "text/.*,application/pdf");
        DropboxDataStore.Config config = new DropboxDataStore.Config(paramMap);
        assertEquals(2, config.supportedMimeTypes.length);
        assertEquals("text/.*", config.supportedMimeTypes[0]);
        assertEquals("application/pdf", config.supportedMimeTypes[1]);
    }

    public void test_Config_fields() throws Exception {
        DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fields", "field1, field2, field3");
        DropboxDataStore.Config config = new DropboxDataStore.Config(paramMap);
        assertEquals(3, config.fields.length);
        assertEquals("field1", config.fields[0]);
        assertEquals("field2", config.fields[1]);
        assertEquals("field3", config.fields[2]);
    }

    public void test_Config_toString() throws Exception {
        DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("max_size", "5000000");
        paramMap.put("ignore_folder", "false");
        DropboxDataStore.Config config = new DropboxDataStore.Config(paramMap);
        String str = config.toString();
        assertNotNull(str);
        assertTrue(str.contains("maxSize=5000000"));
        assertTrue(str.contains("ignoreFolder=false"));
    }

    /** Client double that lists nothing and never touches the network. */
    private static class EmptyClient extends DropboxClient {
        EmptyClient(final DataStoreParams params) {
            super(params);
        }

        @Override
        public void listFiles(final String path, final boolean crawlPapers, final Consumer<Metadata> consumer) throws DbxException {
            // nothing to crawl
        }
    }

    @Test
    public void test_storeData_basicPlan_nothingToCrawl_returnsWithoutWaiting() throws Exception {
        final DropboxDataStore store = new DropboxDataStore() {
            @Override
            protected DropboxClient createClient(final DataStoreParams paramMap) {
                return new EmptyClient(paramMap);
            }
        };
        final DataStoreParams params = new DataStoreParams();
        params.put(DropboxClient.ACCESS_TOKEN, "test_token");
        params.put(DropboxDataStore.BASIC_PLAN, "true");

        final long start = System.nanoTime();
        store.storeData(null, null, params, new java.util.HashMap<>(), new java.util.HashMap<>());
        final long elapsed = (System.nanoTime() - start) / 1_000_000L;
        assertTrue("elapsed=" + elapsed + "ms", elapsed < 20_000L);
    }

    /**
     * {@link Constants#CRAWLER_STATS_KEY} identifies one document, so it must reach
     * {@code callback.store} on a per-document copy and never be written to the
     * {@link DataStoreParams} instance that every worker thread shares.
     */
    @Test
    public void test_storeFile_statsKeyIsOnPerDocumentCopy() throws Exception {
        final CrawlerStatsHelper crawlerStatsHelper = new CrawlerStatsHelper();
        crawlerStatsHelper.init();
        ComponentUtil.register(crawlerStatsHelper, "crawlerStatsHelper");
        ComponentUtil.register(new SystemHelper(), "systemHelper");
        ComponentUtil.register(new PermissionHelper(), "permissionHelper");
        ComponentUtil.register(new FailureUrlService() {
            @Override
            public FailureUrl store(final CrawlingConfig crawlingConfig, final String errorName, final String url, final Throwable e) {
                throw new AssertionError("unexpected failure: " + errorName, e);
            }
        }, FailureUrlService.class.getCanonicalName());

        final DataStoreParams params = new DataStoreParams();
        params.put(DropboxDataStore.IGNORE_FOLDER, "false");
        params.put("custom", "value");

        final List<DataStoreParams> stored = new ArrayList<>();
        final IndexUpdateCallback callback = new IndexUpdateCallback() {
            @Override
            public void store(final DataStoreParams paramMap, final Map<String, Object> dataMap) {
                stored.add(paramMap);
            }

            @Override
            public long getDocumentSize() {
                return 0;
            }

            @Override
            public long getExecuteTime() {
                return 0;
            }

            @Override
            public void commit() {
            }
        };

        final DropboxDataStore store = new DropboxDataStore();
        final DropboxDataStore.Config config = new DropboxDataStore.Config(params);
        for (final String path : new String[] { "/first", "/second" }) {
            store.storeFile(null, callback, params, new HashMap<>(), new HashMap<>(), config, null, null, null, null, path,
                    FolderMetadata.newBuilder(path.substring(1), "id:" + path).withPathLower(path).withPathDisplay(path).build(),
                    new ArrayList<>());
        }

        assertEquals(2, stored.size());
        assertNull("shared params must not carry the stats key", params.get(Constants.CRAWLER_STATS_KEY));
        assertNotSame(stored.get(0), stored.get(1));
        for (int i = 0; i < 2; i++) {
            final DataStoreParams localParams = stored.get(i);
            assertNotSame(params, localParams);
            assertEquals("value", localParams.getAsString("custom"));
            assertTrue(localParams.get(Constants.CRAWLER_STATS_KEY) instanceof StatsKeyObject);
        }
        assertNotSame(stored.get(0).get(Constants.CRAWLER_STATS_KEY), stored.get(1).get(Constants.CRAWLER_STATS_KEY));
    }

}
