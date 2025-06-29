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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Consumer;

import org.apache.commons.io.output.DeferredFileOutputStream;
import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.util.TemporaryFileInputStream;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;

import com.dropbox.core.DbxDownloader;
import com.dropbox.core.DbxException;
import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.DbxTeamClientV2;
import com.dropbox.core.v2.common.PathRoot;
import com.dropbox.core.v2.files.ExportResult;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.FolderMetadata;
import com.dropbox.core.v2.files.ListFolderResult;
import com.dropbox.core.v2.files.Metadata;
import com.dropbox.core.v2.paper.ExportFormat;
import com.dropbox.core.v2.paper.ListPaperDocsResponse;
import com.dropbox.core.v2.paper.PaperDocExportResult;
import com.dropbox.core.v2.team.AdminTier;
import com.dropbox.core.v2.team.TeamFolderListResult;
import com.dropbox.core.v2.team.TeamFolderMetadata;
import com.dropbox.core.v2.team.TeamMemberInfo;

public class DropboxClient {

    private static final Logger logger = LogManager.getLogger(DropboxClient.class);

    protected static final String APP_KEY = "app_key";
    protected static final String APP_SECRET = "app_secret";
    protected static final String ACCESS_TOKEN = "access_token";

    protected static final String MAX_CACHED_CONTENT_SIZE = "max_cached_content_size";

    protected DbxRequestConfig config;
    protected DbxClientV2 basicClient;
    protected DbxTeamClientV2 teamClient;
    protected DataStoreParams params;

    protected int maxCachedContentSize = 1024 * 1024;

    public DropboxClient(final DataStoreParams params) {
        this.params = params;

        final String accessToken = params.getAsString(ACCESS_TOKEN, StringUtil.EMPTY);
        if (StringUtil.isBlank(accessToken)) {
            throw new DataStoreException("Parameter '" + ACCESS_TOKEN + "' is required");
        }

        this.config = new DbxRequestConfig("fess");
        this.basicClient = new DbxClientV2(config, accessToken);
        this.teamClient = new DbxTeamClientV2(config, accessToken);

        final String size = params.getAsString(MAX_CACHED_CONTENT_SIZE);
        if (StringUtil.isNotBlank(size)) {
            maxCachedContentSize = Integer.parseInt(size);
        }
    }

    public void getMembers(final Consumer<TeamMemberInfo> consumer) throws DbxException {
        teamClient.team().membersList().getMembers().forEach(consumer);
    }

    public List<TeamMemberInfo> getMembers() throws DbxException {
        return teamClient.team().membersList().getMembers();
    }

    public void getMemberFiles(final String memberId, final String path, final boolean crawlPapers, final Consumer<Metadata> consumer)
            throws DbxException {
        ListFolderResult listFolderResult = teamClient.asMember(memberId).files().listFolderBuilder(path).withRecursive(true).start();
        while (true) {
            for (final Metadata file : listFolderResult.getEntries()) {
                if (crawlPapers) {
                    if (file.getName().endsWith(".paper")) {
                        // process only paper files (DropboxPaperDataStore)
                        consumer.accept(file);
                    }
                } else if (!file.getName().endsWith(".paper")) {
                    // process files except paper files (DropboxDataStore)
                    consumer.accept(file);
                }
            }
            if (!listFolderResult.getHasMore()) {
                break;
            }
            listFolderResult = teamClient.asMember(memberId).files().listFolderContinue(listFolderResult.getCursor());
        }
    }

    public void getMemberPaperIds(final String memberId, final Consumer<String> consumer) throws DbxException {
        ListPaperDocsResponse listPaperDocsResponse = teamClient.asMember(memberId).paper().docsListBuilder().start();
        while (true) {
            listPaperDocsResponse.getDocIds().forEach(consumer);
            if (!listPaperDocsResponse.getHasMore()) {
                break;
            }
            listPaperDocsResponse = teamClient.asMember(memberId).paper().docsListContinue(listPaperDocsResponse.getCursor().getValue());
        }
    }

    public void getTeamFolders(final Consumer<TeamFolderMetadata> consumer) throws DbxException {
        TeamFolderListResult teamFolderListResult = teamClient.team().teamFolderList();
        while (true) {
            teamFolderListResult.getTeamFolders().forEach(consumer);
            if (!teamFolderListResult.getHasMore()) {
                break;
            }
            teamFolderListResult = teamClient.team().teamFolderListContinue(teamFolderListResult.getCursor());
        }
    }

    public void getTeamFiles(final String adminId, final String teamFolderId, final Consumer<Metadata> consumer) throws DbxException {
        getTeamFiles(adminId, teamFolderId, "", true, consumer);
    }

    public void getTeamFiles(final String adminId, final String teamFolderId, final String path, final boolean recursive,
            final Consumer<Metadata> consumer) throws DbxException {
        final DbxClientV2 clientV2 = teamClient.asAdmin(adminId).withPathRoot(PathRoot.namespaceId(teamFolderId));
        ListFolderResult listFolderResult =
                clientV2.files().listFolderBuilder("ns:" + teamFolderId + path).withRecursive(recursive).start();
        while (true) {
            listFolderResult.getEntries().forEach(consumer);
            if (!listFolderResult.getHasMore()) {
                break;
            }
            listFolderResult = clientV2.files().listFolderContinue(listFolderResult.getCursor());
        }
    }

    public void listFiles(final String path, final boolean crawlPapers, final Consumer<Metadata> consumer) throws DbxException {
        ListFolderResult listFolderResult = basicClient.files().listFolder(path);
        while (true) {
            for (final Metadata file : listFolderResult.getEntries()) {
                if (crawlPapers) {
                    if (file instanceof FolderMetadata) {
                        consumer.accept(file);
                    } else if (file.getName().endsWith(".paper")) {
                        // process only paper files (DropboxPaperDataStore)
                        consumer.accept(file);
                    }
                } else if (!file.getName().endsWith(".paper")) {
                    // process files except paper files (DropboxDataStore)
                    consumer.accept(file);
                }
            }
            if (!listFolderResult.getHasMore()) {
                break;
            }
            listFolderResult = basicClient.files().listFolderContinue(listFolderResult.getCursor());
        }
    }

    public InputStream getFileInputStream(final FileMetadata file) throws DbxException {
        try (final ByteArrayOutputStream dfos = new ByteArrayOutputStream()) {
            basicClient.files().downloadBuilder(file.getPathLower()).download(dfos);
            dfos.flush();
            String content = dfos.toString(StandardCharsets.UTF_8);
            logger.info("Content: " + content);
        } catch (final IOException e) {
            throw new CrawlingAccessException("Failed to create an input stream from " + file.getId(), e);
        }

        try (final DeferredFileOutputStream dfos =
                new DeferredFileOutputStream(maxCachedContentSize, "crawler-DropboxClient-", ".out", SystemUtils.getJavaIoTmpDir())) {
            basicClient.files().downloadBuilder(file.getPathLower()).download(dfos);
            dfos.flush();
            if (dfos.isInMemory()) {
                return new ByteArrayInputStream(dfos.getData());
            }
            return new TemporaryFileInputStream(dfos.getFile());
        } catch (final IOException e) {
            throw new CrawlingAccessException("Failed to create an input stream from " + file.getId(), e);
        }
    }

    public InputStream getMemberFileInputStream(final String memberId, final FileMetadata file) throws DbxException {
        try (final DeferredFileOutputStream dfos =
                new DeferredFileOutputStream(maxCachedContentSize, "crawler-DropboxClient-", ".out", SystemUtils.getJavaIoTmpDir())) {
            teamClient.asMember(memberId).files().download(file.getPathDisplay()).download(dfos);
            dfos.flush();
            if (dfos.isInMemory()) {
                return new ByteArrayInputStream(dfos.getData());
            }
            return new TemporaryFileInputStream(dfos.getFile());
        } catch (final IOException e) {
            throw new CrawlingAccessException("Failed to create an input stream from " + file.getId(), e);
        }
    }

    public InputStream getTeamFileInputStream(final String adminId, final String teamFolderId, final FileMetadata file)
            throws DbxException {
        try (final DeferredFileOutputStream dfos =
                new DeferredFileOutputStream(maxCachedContentSize, "crawler-DropboxClient-", ".out", SystemUtils.getJavaIoTmpDir())) {
            teamClient.asAdmin(adminId).withPathRoot(PathRoot.namespaceId(teamFolderId)).files().download(file.getPathDisplay())
                    .download(dfos);
            dfos.flush();
            if (dfos.isInMemory()) {
                return new ByteArrayInputStream(dfos.getData());
            }
            return new TemporaryFileInputStream(dfos.getFile());
        } catch (final IOException e) {
            throw new CrawlingAccessException("Failed to create an input stream from " + file.getId(), e);
        }
    }

    public DbxDownloader<PaperDocExportResult> getPaperDownloader(final String memberId, final String docId) throws DbxException {
        return teamClient.asMember(memberId).paper().docsDownload(docId, ExportFormat.MARKDOWN);
    }

    // Added method for basic plan using file export API instead of docsDownload.
    public DbxDownloader<ExportResult> getBasicExporter(final String path) throws DbxException {
        return basicClient.files().export(path, "markdown");
    }

    // Added method for basic plan (when memberId is null) using basicClient
    public DbxDownloader<PaperDocExportResult> getBasicPaperDownloader(final String docId) throws DbxException {
        return basicClient.paper().docsDownload(docId, ExportFormat.MARKDOWN);
    }

    public TeamMemberInfo getAdmin() throws DbxException {
        return getAdmin(teamClient.team().membersList().getMembers());
    }

    public TeamMemberInfo getAdmin(final List<TeamMemberInfo> members) {
        for (final TeamMemberInfo member : members) {
            if (member.getRole() == AdminTier.TEAM_ADMIN) {
                return member;
            }
        }
        throw new DataStoreException("Admin is not found");
    }
}
