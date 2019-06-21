/*
 * Copyright 2012-2019 CodeLibs Project and the Others.
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

import com.dropbox.core.DbxException;
import org.dbflute.utflute.lastaflute.LastaFluteTestCase;

import java.util.HashMap;
import java.util.Map;

public class DropboxClientTest extends LastaFluteTestCase {

    private static final String ACCESS_TOKEN = "";

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void test() throws Exception {
        // getMembers();
    }

    private void getMembers() throws DbxException {
        final Map<String, String> params = new HashMap<>();
        params.put(DropboxClient.ACCESS_TOKEN, ACCESS_TOKEN);
        final DropboxClient client = new DropboxClient(params);
        client.getMembers(info -> System.out.println(info.getProfile().getName().getDisplayName()));
    }

    private void getMetadata(){

    }
}
