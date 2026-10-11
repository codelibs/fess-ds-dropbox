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

import java.io.IOException;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.util.ComponentUtil;
import org.codelibs.fess.ds.dropbox.UnitDsTestCase;

import com.dropbox.core.BadResponseCodeException;
import com.dropbox.core.DbxException;
import com.dropbox.core.NetworkIOException;
import com.dropbox.core.v2.files.Metadata;
import com.dropbox.core.v2.team.TeamMemberInfo;

public class DropboxPaperDataStoreTest extends UnitDsTestCase {
    public DropboxPaperDataStore dataStore;

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
        dataStore = new DropboxPaperDataStore();
    }

    @Override
    public void tearDown(TestInfo testInfo) throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown(testInfo);
    }

    public void test_getName() throws Exception {
        assertEquals("DropboxPaperDataStore", dataStore.getName());
    }

    public void test_getUrlFromId() throws Exception {
        String docId = "abc123xyz";
        String url = dataStore.getUrlFromId(docId);
        assertEquals("https://paper.dropbox.com/doc/abc123xyz", url);
    }

    public void test_getUrlFromId_withSpecialCharacters() throws Exception {
        String docId = "doc-id-with-dashes";
        String url = dataStore.getUrlFromId(docId);
        assertEquals("https://paper.dropbox.com/doc/doc-id-with-dashes", url);
    }

    public void test_getUrlFromId_withNumbers() throws Exception {
        String docId = "12345";
        String url = dataStore.getUrlFromId(docId);
        assertEquals("https://paper.dropbox.com/doc/12345", url);
    }

    public void test_getUrlFromPath() throws Exception {
        String path = "/Documents/paper.paper";
        String url = dataStore.getUrlFromPath(path);
        assertEquals("https://www.dropbox.com/home/Documents/paper.paper", url);
    }

    public void test_getUrlFromPath_withSpaces() throws Exception {
        String path = "/My Documents/Test Paper.paper";
        String url = dataStore.getUrlFromPath(path);
        assertTrue(url.startsWith("https://www.dropbox.com/home/"));
        assertTrue(url.contains("My%20Documents"));
        assertTrue(url.contains("Test%20Paper.paper"));
    }

    public void test_getUrlFromPath_root() throws Exception {
        String path = "/test.paper";
        String url = dataStore.getUrlFromPath(path);
        assertEquals("https://www.dropbox.com/home/test.paper", url);
    }

    public void test_getUrlFromPath_nested() throws Exception {
        String path = "/folder1/folder2/folder3/document.paper";
        String url = dataStore.getUrlFromPath(path);
        assertEquals("https://www.dropbox.com/home/folder1/folder2/folder3/document.paper", url);
    }

    public void test_getUrlFromPath_withSpecialCharacters() throws Exception {
        String path = "/test & document.paper";
        String url = dataStore.getUrlFromPath(path);
        assertTrue(url.startsWith("https://www.dropbox.com/home/"));
        assertTrue(url.contains("%20&%20"));
    }

    public void test_getUrlFromPath_withJapanese() throws Exception {
        String path = "/ドキュメント/テスト.paper";
        String url = dataStore.getUrlFromPath(path);
        assertNotNull(url);
        assertTrue(url.startsWith("https://www.dropbox.com/home/"));
    }

    public void test_getUrlFromId_longId() throws Exception {
        String docId = "veryLongDocumentIdWith1234567890AndManyCharacters";
        String url = dataStore.getUrlFromId(docId);
        assertEquals("https://paper.dropbox.com/doc/veryLongDocumentIdWith1234567890AndManyCharacters", url);
    }

    public void test_getUrlFromPath_multipleSpaces() throws Exception {
        String path = "/folder  with  spaces/file.paper";
        String url = dataStore.getUrlFromPath(path);
        assertNotNull(url);
        assertTrue(url.startsWith("https://www.dropbox.com/home/"));
    }

    public void test_params_basicPlan() throws Exception {
        assertEquals("basic_plan", DropboxPaperDataStore.BASIC_PLAN);
    }

    public void test_extractorName() throws Exception {
        assertEquals("tikaExtractor", dataStore.extractorName);
    }

    public void test_extractorName_canBeModified() throws Exception {
        dataStore.extractorName = "customExtractor";
        assertEquals("customExtractor", dataStore.extractorName);
    }

    /** Client double that never touches the network. */
    private static class StubClient extends DropboxClient {
        private final DbxException failure;

        StubClient(final DataStoreParams params, final DbxException failure) {
            super(params);
            this.failure = failure;
        }

        @Override
        public void getMembers(final Consumer<TeamMemberInfo> consumer) throws DbxException {
            if (failure != null) {
                throw failure;
            }
        }

        @Override
        public void listFiles(final String path, final boolean crawlPapers, final Consumer<Metadata> consumer) throws DbxException {
            if (failure != null) {
                throw failure;
            }
        }
    }

    private DropboxPaperDataStore newDataStore(final DbxException failure) {
        return new DropboxPaperDataStore() {
            @Override
            protected DropboxClient createClient(final DataStoreParams paramMap) {
                return new StubClient(paramMap, failure);
            }
        };
    }

    private DataStoreParams newParams(final boolean basicPlan) {
        final DataStoreParams params = new DataStoreParams();
        params.put(DropboxClient.ACCESS_TOKEN, "secret-token-value");
        params.put(DropboxPaperDataStore.BASIC_PLAN, String.valueOf(basicPlan));
        return params;
    }

    private long storeDataMillis(final DropboxPaperDataStore store, final DataStoreParams params) {
        final long start = System.nanoTime();
        store.storeData(null, null, params, new java.util.HashMap<>(), new java.util.HashMap<>());
        return (System.nanoTime() - start) / 1_000_000L;
    }

    @Test
    public void test_storeData_teamPlan_noPapers_returnsWithoutWaiting() throws Exception {
        final long elapsed = storeDataMillis(newDataStore(null), newParams(false));
        assertTrue("elapsed=" + elapsed + "ms", elapsed < 20_000L);
    }

    @Test
    public void test_storeData_basicPlan_noPapers_returnsWithoutWaiting() throws Exception {
        final long elapsed = storeDataMillis(newDataStore(null), newParams(true));
        assertTrue("elapsed=" + elapsed + "ms", elapsed < 20_000L);
    }

    @Test
    public void test_storeData_teamPlan_membersFailure_isReported() throws Exception {
        final DbxException failure = new NetworkIOException(new IOException("unreachable"));
        try {
            newDataStore(failure).storeData(null, null, newParams(false), new java.util.HashMap<>(), new java.util.HashMap<>());
            fail("A failure to list team members must not be swallowed");
        } catch (final DataStoreException e) {
            assertSame(failure, e.getCause());
            assertTrue(e.getMessage(), e.getMessage().contains("NetworkIOException"));
            assertTrue(e.getMessage(), e.getMessage().contains("IOException"));
            assertFalse(e.getMessage(), e.getMessage().contains("secret-token-value"));
        }
    }

    @Test
    public void test_storeData_basicPlan_listFailure_doesNotThrow() throws Exception {
        final long elapsed = storeDataMillis(newDataStore(new BadResponseCodeException("req", "Bad", 503)), newParams(true));
        assertTrue("elapsed=" + elapsed + "ms", elapsed < 20_000L);
    }

    @Test
    public void test_describe() throws Exception {
        assertEquals("BadResponseCodeException (HTTP 503)", dataStore.describe(new BadResponseCodeException("req", "Bad", 503)));
        assertEquals("NetworkIOException (UnknownHostException)",
                dataStore.describe(new NetworkIOException(new java.net.UnknownHostException("api.dropboxapi.com"))));
        assertEquals("DbxException", dataStore.describe(new DbxException("x")));
    }

}
