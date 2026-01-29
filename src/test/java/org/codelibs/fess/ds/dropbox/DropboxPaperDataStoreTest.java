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

import org.junit.jupiter.api.TestInfo;

import org.codelibs.fess.util.ComponentUtil;
import org.codelibs.fess.ds.dropbox.UnitDsTestCase;

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

}
