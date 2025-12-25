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

import java.util.ArrayList;
import java.util.List;

import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.dbflute.utflute.lastaflute.LastaFluteTestCase;

import com.dropbox.core.v2.team.AdminTier;
import com.dropbox.core.v2.team.TeamMemberInfo;
import com.dropbox.core.v2.team.TeamMemberProfile;
import com.dropbox.core.v2.team.TeamMemberStatus;
import com.dropbox.core.v2.team.TeamMembershipType;
import com.dropbox.core.v2.users.Name;

public class DropboxClientTest extends LastaFluteTestCase {

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

    public void test_constructor_withValidAccessToken() throws Exception {
        final DataStoreParams params = new DataStoreParams();
        params.put(DropboxClient.ACCESS_TOKEN, "test_token_123");
        final DropboxClient client = new DropboxClient(params);
        assertNotNull(client);
        assertNotNull(client.config);
        assertNotNull(client.basicClient);
        assertNotNull(client.teamClient);
    }

    public void test_constructor_withoutAccessToken() throws Exception {
        final DataStoreParams params = new DataStoreParams();
        try {
            new DropboxClient(params);
            fail("Should throw DataStoreException when access_token is missing");
        } catch (DataStoreException e) {
            assertTrue(e.getMessage().contains("access_token"));
            assertTrue(e.getMessage().contains("required"));
        }
    }

    public void test_constructor_withEmptyAccessToken() throws Exception {
        final DataStoreParams params = new DataStoreParams();
        params.put(DropboxClient.ACCESS_TOKEN, "");
        try {
            new DropboxClient(params);
            fail("Should throw DataStoreException when access_token is empty");
        } catch (DataStoreException e) {
            assertTrue(e.getMessage().contains("access_token"));
        }
    }

    public void test_constructor_withMaxCachedContentSize() throws Exception {
        final DataStoreParams params = new DataStoreParams();
        params.put(DropboxClient.ACCESS_TOKEN, "test_token");
        params.put(DropboxClient.MAX_CACHED_CONTENT_SIZE, "2097152");
        final DropboxClient client = new DropboxClient(params);
        assertNotNull(client);
        assertEquals(2097152, client.maxCachedContentSize);
    }

    public void test_constructor_defaultMaxCachedContentSize() throws Exception {
        final DataStoreParams params = new DataStoreParams();
        params.put(DropboxClient.ACCESS_TOKEN, "test_token");
        final DropboxClient client = new DropboxClient(params);
        assertEquals(1024 * 1024, client.maxCachedContentSize);
    }

    public void test_getAdmin_findAdmin() throws Exception {
        final DataStoreParams params = new DataStoreParams();
        params.put(DropboxClient.ACCESS_TOKEN, "test_token");
        final DropboxClient client = new DropboxClient(params);

        List<TeamMemberInfo> members = new ArrayList<>();
        // Create member without admin role
        Name name1 = new Name("User", "One", "User One", "User One", "UO");
        TeamMemberProfile profile1 = new TeamMemberProfile("member1", "user@example.com", true, TeamMemberStatus.ACTIVE, name1,
                TeamMembershipType.FULL, new ArrayList<>(), "folder1");
        members.add(new TeamMemberInfo(profile1, AdminTier.MEMBER_ONLY));

        // Create admin member
        Name name2 = new Name("Admin", "User", "Admin User", "Admin User", "AU");
        TeamMemberProfile profile2 = new TeamMemberProfile("admin1", "admin@example.com", true, TeamMemberStatus.ACTIVE, name2,
                TeamMembershipType.FULL, new ArrayList<>(), "folder2");
        members.add(new TeamMemberInfo(profile2, AdminTier.TEAM_ADMIN));

        TeamMemberInfo admin = client.getAdmin(members);
        assertNotNull(admin);
        assertEquals(AdminTier.TEAM_ADMIN, admin.getRole());
        assertEquals("admin1", admin.getProfile().getTeamMemberId());
    }

    public void test_getAdmin_noAdmin() throws Exception {
        final DataStoreParams params = new DataStoreParams();
        params.put(DropboxClient.ACCESS_TOKEN, "test_token");
        final DropboxClient client = new DropboxClient(params);

        List<TeamMemberInfo> members = new ArrayList<>();
        // Create only non-admin members
        Name name1 = new Name("User", "One", "User One", "User One", "UO");
        TeamMemberProfile profile1 = new TeamMemberProfile("member1", "user1@example.com", true, TeamMemberStatus.ACTIVE, name1,
                TeamMembershipType.FULL, new ArrayList<>(), "folder1");
        members.add(new TeamMemberInfo(profile1, AdminTier.MEMBER_ONLY));

        Name name2 = new Name("User", "Two", "User Two", "User Two", "UT");
        TeamMemberProfile profile2 = new TeamMemberProfile("member2", "user2@example.com", true, TeamMemberStatus.ACTIVE, name2,
                TeamMembershipType.FULL, new ArrayList<>(), "folder2");
        members.add(new TeamMemberInfo(profile2, AdminTier.MEMBER_ONLY));

        try {
            client.getAdmin(members);
            fail("Should throw DataStoreException when no admin is found");
        } catch (DataStoreException e) {
            assertTrue(e.getMessage().contains("Admin is not found"));
        }
    }

    public void test_getAdmin_emptyList() throws Exception {
        final DataStoreParams params = new DataStoreParams();
        params.put(DropboxClient.ACCESS_TOKEN, "test_token");
        final DropboxClient client = new DropboxClient(params);

        List<TeamMemberInfo> members = new ArrayList<>();
        try {
            client.getAdmin(members);
            fail("Should throw DataStoreException when member list is empty");
        } catch (DataStoreException e) {
            assertTrue(e.getMessage().contains("Admin is not found"));
        }
    }

    public void test_getAdmin_multipleAdmins() throws Exception {
        final DataStoreParams params = new DataStoreParams();
        params.put(DropboxClient.ACCESS_TOKEN, "test_token");
        final DropboxClient client = new DropboxClient(params);

        List<TeamMemberInfo> members = new ArrayList<>();
        // Create first admin
        Name name1 = new Name("Admin", "One", "Admin One", "Admin One", "AO");
        TeamMemberProfile profile1 = new TeamMemberProfile("admin1", "admin1@example.com", true, TeamMemberStatus.ACTIVE, name1,
                TeamMembershipType.FULL, new ArrayList<>(), "folder1");
        members.add(new TeamMemberInfo(profile1, AdminTier.TEAM_ADMIN));

        // Create second admin
        Name name2 = new Name("Admin", "Two", "Admin Two", "Admin Two", "AT");
        TeamMemberProfile profile2 = new TeamMemberProfile("admin2", "admin2@example.com", true, TeamMemberStatus.ACTIVE, name2,
                TeamMembershipType.FULL, new ArrayList<>(), "folder2");
        members.add(new TeamMemberInfo(profile2, AdminTier.TEAM_ADMIN));

        // Should return the first admin found
        TeamMemberInfo admin = client.getAdmin(members);
        assertNotNull(admin);
        assertEquals(AdminTier.TEAM_ADMIN, admin.getRole());
        assertEquals("admin1", admin.getProfile().getTeamMemberId());
    }

    public void test_params_appKey() throws Exception {
        assertEquals("app_key", DropboxClient.APP_KEY);
    }

    public void test_params_appSecret() throws Exception {
        assertEquals("app_secret", DropboxClient.APP_SECRET);
    }

    public void test_params_accessToken() throws Exception {
        assertEquals("access_token", DropboxClient.ACCESS_TOKEN);
    }

    public void test_params_maxCachedContentSize() throws Exception {
        assertEquals("max_cached_content_size", DropboxClient.MAX_CACHED_CONTENT_SIZE);
    }

    /*
     * Note: The following methods require actual Dropbox API access and are commented out
     * for unit testing. They can be used for manual integration testing with a valid access token.
     *
    private static final String ACCESS_TOKEN = "YOUR_ACCESS_TOKEN_HERE";

    private void integrationTest_getMembers() throws DbxException {
        final DataStoreParams params = new DataStoreParams();
        params.put(DropboxClient.ACCESS_TOKEN, ACCESS_TOKEN);
        final DropboxClient client = new DropboxClient(params);
        client.getMembers(info -> System.out.println(info.getProfile().getName().getDisplayName()));
    }

    private void integrationTest_getMemberFiles() throws DbxException {
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

    private void integrationTest_getTeamFiles() throws DbxException {
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

    private void integrationTest_getMemberPaperIds() throws DbxException {
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

    private void integrationTest_getFileInputStream() throws DbxException {
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
                            String content = IOUtils.toString(client.getMemberFileInputStream(memberId, file), StandardCharsets.UTF_8);
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

    private void integrationTest_getTeamFileInputStream() throws DbxException {
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
    */

}
