/*
 * Copyright 2012-2023 CodeLibs Project and the Others.
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

import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.codelibs.fess.entity.DataStoreParams;
import org.dbflute.utflute.lastaflute.LastaFluteTestCase;

import com.dropbox.core.DbxException;
import com.dropbox.core.v2.files.FileMetadata;

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
        // getMemberFiles();
        // getTeamFiles();
        // getMemberPaperIds();
        // getFileInputStream();
        // getTeamFileInputStream();
    }

    private void getMembers() throws DbxException {
        final DataStoreParams params = new DataStoreParams();
        params.put(DropboxClient.ACCESS_TOKEN, ACCESS_TOKEN);
        final DropboxClient client = new DropboxClient(params);
        client.getMembers(info -> System.out.println(info.getProfile().getName().getDisplayName()));
    }

    private void getMemberFiles() throws DbxException {
        final DataStoreParams params = new DataStoreParams();
        params.put(DropboxClient.ACCESS_TOKEN, ACCESS_TOKEN);
        final DropboxClient client = new DropboxClient(params);
        client.getMembers(info -> {
            try {
                System.out.println(info.getProfile().getName().getDisplayName() + "'s Files");
                client.getMemberFiles(info.getProfile().getTeamMemberId(), "", false,
                        metadata -> System.out.println("  " + metadata.getPathDisplay()));
            } catch (final DbxException e) {
                e.printStackTrace();
            }
        });
    }

    private void getTeamFiles() throws DbxException {
        final DataStoreParams params = new DataStoreParams();
        params.put(DropboxClient.ACCESS_TOKEN, ACCESS_TOKEN);
        final DropboxClient client = new DropboxClient(params);
        final String adminId = client.getAdmin().getProfile().getTeamMemberId();
        client.getTeamFolders(folder -> {
            try {
                client.getTeamFiles(adminId, folder.getTeamFolderId(), metadata -> System.out.println(metadata.getPathDisplay()));
            } catch (final DbxException e) {
                e.printStackTrace();
            }
        });
    }

    private void getMemberPaperIds() throws DbxException {
        final DataStoreParams params = new DataStoreParams();
        params.put(DropboxClient.ACCESS_TOKEN, ACCESS_TOKEN);
        final DropboxClient client = new DropboxClient(params);
        client.getMembers(info -> {
            try {
                System.out.println(info.getProfile().getName().getDisplayName() + "'s Papers");
                client.getMemberPaperIds(info.getProfile().getTeamMemberId(), System.out::println);
            } catch (final DbxException e) {
                e.printStackTrace();
            }
        });
    }

    private void getFileInputStream() throws DbxException {
        final DataStoreParams params = new DataStoreParams();
        params.put(DropboxClient.ACCESS_TOKEN, ACCESS_TOKEN);
        final DropboxClient client = new DropboxClient(params);
        client.getMembers(info -> {
            try {
                System.out.println(info.getProfile().getName().getDisplayName() + "'s Files");
                final String memberId = info.getProfile().getTeamMemberId();
                client.getMemberFiles(memberId, "", false, metadata -> {
                    System.out.println("  " + metadata.getPathDisplay());
                    if (metadata instanceof FileMetadata) {
                        final FileMetadata file = (FileMetadata) metadata;
                        try {
                            String content = IOUtils.toString(client.getFileInputStream(memberId, file), StandardCharsets.UTF_8);
                            content = content.replaceAll("\\r\\n|\\r|\\n", "");
                            content = content.substring(0, Math.min(20, content.length()));
                            System.out.println("    " + content);
                        } catch (final Exception e) {
                            System.out.println("    Failed by 'restricted_content'");
                        }
                    }
                });
            } catch (final DbxException e) {
                e.printStackTrace();
            }
        });
    }

    private void getTeamFileInputStream() throws DbxException {
        final DataStoreParams params = new DataStoreParams();
        params.put(DropboxClient.ACCESS_TOKEN, ACCESS_TOKEN);
        final DropboxClient client = new DropboxClient(params);
        final String adminId = client.getAdmin().getProfile().getTeamMemberId();
        client.getTeamFolders(folder -> {
            final String teamFolderId = folder.getTeamFolderId();
            try {
                client.getTeamFiles(adminId, teamFolderId, metadata -> {
                    System.out.println(metadata.getPathDisplay());
                    if (metadata instanceof FileMetadata) {
                        final FileMetadata file = (FileMetadata) metadata;
                        try {
                            String content =
                                    IOUtils.toString(client.getTeamFileInputStream(adminId, teamFolderId, file), StandardCharsets.UTF_8);
                            content = content.replaceAll("\\r\\n|\\r|\\n", "");
                            content = content.substring(0, Math.min(20, content.length()));
                            System.out.println("  " + content);
                        } catch (final Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            } catch (final DbxException e) {
                e.printStackTrace();
            }
        });
    }

}
