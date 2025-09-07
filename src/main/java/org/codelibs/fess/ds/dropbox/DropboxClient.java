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

/**
 * This class provides a client for accessing Dropbox APIs.
 * It supports both individual and team accounts.
 */
public class DropboxClient {

    private static final Logger logger = LogManager.getLogger(DropboxClient.class);

    /** Key for the App Key parameter. */
    protected static final String APP_KEY = "app_key";
    /** Key for the App Secret parameter. */
    protected static final String APP_SECRET = "app_secret";
    /** Key for the Access Token parameter. */
    protected static final String ACCESS_TOKEN = "access_token";

    /** Key for the maximum cached content size parameter. */
    protected static final String MAX_CACHED_CONTENT_SIZE = "max_cached_content_size";

    /** Dropbox request configuration. */
    protected DbxRequestConfig config;
    /** Dropbox client for basic accounts. */
    protected DbxClientV2 basicClient;
    /** Dropbox client for team accounts. */
    protected DbxTeamClientV2 teamClient;
    /** DataStore parameters. */
    protected DataStoreParams params;

    /** Maximum size of content to be cached in memory. */
    protected int maxCachedContentSize = 1024 * 1024;

    /**
     * Constructs a new DropboxClient with the given DataStore parameters.
     *
     * @param params The DataStore parameters for configuration.
     * @throws DataStoreException If the access token is not provided.
     */
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

    /**
     * Retrieves all members of the team and processes them with the given consumer.
     *
     * @param consumer A consumer to process each team member.
     * @throws DbxException If an error occurs while accessing the Dropbox API.
     */
    public void getMembers(final Consumer<TeamMemberInfo> consumer) throws DbxException {
        teamClient.team().membersList().getMembers().forEach(consumer);
    }

    /**
     * Retrieves a list of all members in the team.
     *
     * @return A list of team members.
     * @throws DbxException If an error occurs while accessing the Dropbox API.
     */
    public List<TeamMemberInfo> getMembers() throws DbxException {
        return teamClient.team().membersList().getMembers();
    }

    /**
     * Retrieves files and folders for a specific team member and processes them.
     *
     * @param memberId The ID of the team member.
     * @param path The path to list files from.
     * @param crawlPapers Whether to include Paper documents.
     * @param consumer A consumer to process each metadata entry.
     * @throws DbxException If an error occurs while accessing the Dropbox API.
     */
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

    /**
     * Retrieves Paper document IDs for a specific team member.
     *
     * @param memberId The ID of the team member.
     * @param consumer A consumer to process each Paper document ID.
     * @throws DbxException If an error occurs while accessing the Dropbox API.
     */
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

    /**
     * Retrieves all team folders and processes them.
     *
     * @param consumer A consumer to process each team folder metadata.
     * @throws DbxException If an error occurs while accessing the Dropbox API.
     */
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

    /**
     * Retrieves files from a team folder.
     *
     * @param adminId The ID of the administrator.
     * @param teamFolderId The ID of the team folder.
     * @param consumer A consumer to process each metadata entry.
     * @throws DbxException If an error occurs while accessing the Dropbox API.
     */
    public void getTeamFiles(final String adminId, final String teamFolderId, final Consumer<Metadata> consumer) throws DbxException {
        getTeamFiles(adminId, teamFolderId, "", true, consumer);
    }

    /**
     * Retrieves files from a team folder with more options.
     *
     * @param adminId The ID of the administrator.
     * @param teamFolderId The ID of the team folder.
     * @param path The path within the team folder.
     * @param recursive Whether to list files recursively.
     * @param consumer A consumer to process each metadata entry.
     * @throws DbxException If an error occurs while accessing the Dropbox API.
     */
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

    /**
     * Lists files and folders in a given path for a basic account.
     *
     * @param path The path to list.
     * @param crawlPapers Whether to include Paper documents.
     * @param consumer A consumer to process each metadata entry.
     * @throws DbxException If an error occurs while accessing the Dropbox API.
     */
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

    /**
     * Gets an input stream for a file.
     *
     * @param file The file metadata.
     * @return An input stream for the file content.
     * @throws DbxException If an error occurs while accessing the Dropbox API.
     */
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

    /**
     * Gets an input stream for a file of a specific team member.
     *
     * @param memberId The ID of the team member.
     * @param file The file metadata.
     * @return An input stream for the file content.
     * @throws DbxException If an error occurs while accessing the Dropbox API.
     */
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

    /**
     * Gets an input stream for a file in a team folder.
     *
     * @param adminId The ID of the administrator.
     * @param teamFolderId The ID of the team folder.
     * @param file The file metadata.
     * @return An input stream for the file content.
     * @throws DbxException If an error occurs while accessing the Dropbox API.
     */
    public InputStream getTeamFileInputStream(final String adminId, final String teamFolderId, final FileMetadata file)
            throws DbxException {
        try (final DeferredFileOutputStream dfos =
                new DeferredFileOutputStream(maxCachedContentSize, "crawler-DropboxClient-", ".out", SystemUtils.getJavaIoTmpDir())) {
            teamClient.asAdmin(adminId)
                    .withPathRoot(PathRoot.namespaceId(teamFolderId))
                    .files()
                    .download(file.getPathDisplay())
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

    /**
     * Gets a downloader for a Paper document.
     *
     * @param memberId The ID of the team member.
     * @param docId The ID of the Paper document.
     * @return A downloader for the Paper document.
     * @throws DbxException If an error occurs while accessing the Dropbox API.
     */
    public DbxDownloader<PaperDocExportResult> getPaperDownloader(final String memberId, final String docId) throws DbxException {
        return teamClient.asMember(memberId).paper().docsDownload(docId, ExportFormat.MARKDOWN);
    }

    /**
     * Gets an exporter for a file for basic plan.
     *
     * @param path The path of the file.
     * @return An exporter for the file.
     * @throws DbxException If an error occurs while accessing the Dropbox API.
     */
    public DbxDownloader<ExportResult> getBasicExporter(final String path) throws DbxException {
        return basicClient.files().export(path, "markdown");
    }

    /**
     * Gets a downloader for a Paper document for basic plan.
     *
     * @param docId The ID of the Paper document.
     * @return A downloader for the Paper document.
     * @throws DbxException If an error occurs while accessing the Dropbox API.
     */
    public DbxDownloader<PaperDocExportResult> getBasicPaperDownloader(final String docId) throws DbxException {
        return basicClient.paper().docsDownload(docId, ExportFormat.MARKDOWN);
    }

    /**
     * Gets the administrator of the team.
     *
     * @return The team member info of the administrator.
     * @throws DbxException If an error occurs while accessing the Dropbox API.
     */
    public TeamMemberInfo getAdmin() throws DbxException {
        return getAdmin(teamClient.team().membersList().getMembers());
    }

    /**
     * Finds the administrator from a list of team members.
     *
     * @param members The list of team members.
     * @return The team member info of the administrator.
     * @throws DataStoreException If no administrator is found.
     */
    public TeamMemberInfo getAdmin(final List<TeamMemberInfo> members) {
        for (final TeamMemberInfo member : members) {
            if (member.getRole() == AdminTier.TEAM_ADMIN) {
                return member;
            }
        }
        throw new DataStoreException("Admin is not found");
    }
}
