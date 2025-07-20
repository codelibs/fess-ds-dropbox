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
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.exception.InterruptedRuntimeException;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MaxLengthExceededException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.helper.PermissionHelper;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.lastaflute.di.core.exception.ComponentNotFoundException;

import com.dropbox.core.DbxException;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.FolderMetadata;
import com.dropbox.core.v2.files.Metadata;
import com.dropbox.core.v2.team.TeamMemberInfo;

/**
 * DataStore for Dropbox.
 * It crawls files and folders from Dropbox and indexes them.
 * This class supports both individual and team accounts.
 *
 * The following parameters are available:
 * <ul>
 * <li>basic_plan: Set to "true" for individual accounts, "false" for team accounts. Default is "false".</li>
 * <li>fields: Fields to be indexed.</li>
 * <li>max_size: Maximum size of files to be indexed. Default is 10MB.</li>
 * <li>ignore_folder: Set to "true" to ignore folders. Default is "true".</li>
 * <li>ignore_error: Set to "true" to ignore errors during crawling. Default is "true".</li>
 * <li>supported_mimetypes: Comma-separated list of supported MIME types. Default is ".*".</li>
 * <li>include_pattern: URL patterns to include for crawling.</li>
 * <li>exclude_pattern: URL patterns to exclude from crawling.</li>
 * <li>default_permissions: Default permissions for the indexed documents.</li>
 * <li>number_of_threads: Number of threads to use for crawling. Default is 1.</li>
 * </ul>
 */
public class DropboxDataStore extends AbstractDataStore {

    private static final Logger logger = LogManager.getLogger(DropboxDataStore.class);

    /** Default maximum file size to be indexed (10MB). */
    protected static final long DEFAULT_MAX_SIZE = 10000000L; // 10m

    // parameters
    /** Parameter key for the basic plan flag. */
    protected static final String BASIC_PLAN = "basic_plan";
    /** Parameter key for the fields to be indexed. */
    protected static final String FIELDS = "fields";
    /** Parameter key for the maximum file size. */
    protected static final String MAX_SIZE = "max_size";
    /** Parameter key for ignoring folders. */
    protected static final String IGNORE_FOLDER = "ignore_folder";
    /** Parameter key for ignoring errors. */
    protected static final String IGNORE_ERROR = "ignore_error";
    /** Parameter key for supported MIME types. */
    protected static final String SUPPORTED_MIMETYPES = "supported_mimetypes";
    /** Parameter key for include patterns. */
    protected static final String INCLUDE_PATTERN = "include_pattern";
    /** Parameter key for exclude patterns. */
    protected static final String EXCLUDE_PATTERN = "exclude_pattern";
    /** Parameter key for default permissions. */
    protected static final String DEFAULT_PERMISSIONS = "default_permissions";
    /** Parameter key for the number of threads. */
    protected static final String NUMBER_OF_THREADS = "number_of_threads";

    // scripts
    /** Script key for file data. */
    protected static final String FILE = "file";

    // - common
    /** Field key for the file URL. */
    protected static final String FILE_URL = "url";
    /** Field key for the file roles. */
    protected static final String FILE_ROLES = "roles";

    /** Field key for the file name. */
    protected static final String FILE_NAME = "name";
    /** Field key for the lower-cased file path. */
    protected static final String FILE_PATH_LOWER = "path_lower";
    /** Field key for the display file path. */
    protected static final String FILE_PATH_DISPLAY = "path_display";
    /** Field key for the parent shared folder ID. */
    protected static final String FILE_PARENT_SHARED_FOLDER_ID = "parent_shared_folder_id";

    /** Field key for the file ID. */
    protected static final String FILE_ID = "id";
    /** Field key for property groups. */
    protected static final String FILE_PROPERTY_GROUPS = "property_groups";
    /** Field key for sharing information. */
    protected static final String FILE_SHARING_INFO = "sharing_info";

    // - file
    /** Field key for file content. */
    protected static final String FILE_CONTENTS = "contents";
    /** Field key for the file MIME type. */
    protected static final String FILE_MIMETYPE = "mimetype";
    /** Field key for the file type. */
    protected static final String FILE_FILETYPE = "filetype";

    /** Field key for the client-modified timestamp. */
    protected static final String FILE_CLIENT_MODIFIED = "client_modified";
    /** Field key for the content hash. */
    protected static final String FILE_CONTENT_HASH = "content_hash";
    /** Field key for export information. */
    protected static final String FILE_EXPORT_INFO = "export_info";
    /** Field key for whether the file has explicit shared members. */
    protected static final String FILE_HAS_EXPLICT_SHARED_MEMBERS = "has_explict_shared_members";
    /** Field key for media information. */
    protected static final String FILE_MEDIA_INFO = "media_info";
    /** Field key for the file revision. */
    protected static final String FILE_REV = "rev";
    /** Field key for the server-modified timestamp. */
    protected static final String FILE_SERVER_MODIFIED = "server_modified";
    /** Field key for the file size. */
    protected static final String FILE_SIZE = "size";
    /** Field key for symlink information. */
    protected static final String FILE_SYMLINK_INFO = "symlink_info";

    // - folder
    /** Field key for the shared folder ID. */
    protected static final String FILE_SHARED_FOLDER_ID = "shared_folder_id";

    // other
    /** The name of the extractor to be used for content extraction. */
    protected String extractorName = "tikaExtractor";

    /**
     * Default constructor.
     */
    public DropboxDataStore() {
        super();
    }

    @Override
    protected String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {
        final Config config = new Config(paramMap);
        if (logger.isDebugEnabled()) {
            logger.debug("config: {}", config);
        }
        final ExecutorService executorService =
                Executors.newFixedThreadPool(Integer.parseInt(paramMap.getAsString(NUMBER_OF_THREADS, "1")));

        try {
            final DropboxClient client = createClient(paramMap);
            final Boolean isBasicPlan = Boolean.parseBoolean(paramMap.getAsString(BASIC_PLAN, "false"));
            if (isBasicPlan) {
                crawlBasicFiles(dataConfig, callback, paramMap, scriptMap, defaultDataMap, config, executorService, client, "");
            } else {
                final List<TeamMemberInfo> members = client.getMembers();
                crawlMemberFiles(dataConfig, callback, paramMap, scriptMap, defaultDataMap, config, executorService, client, members);
                crawlTeamFiles(dataConfig, callback, paramMap, scriptMap, defaultDataMap, config, executorService, client, members);
            }
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (final DbxException e) {
            throw new DataStoreException("Dropbox exception occurs.", e);
        } catch (final InterruptedException e) {
            throw new InterruptedRuntimeException(e);
        } finally {
            executorService.shutdown();
        }
    }

    /**
     * Crawls files for a basic (individual) account.
     *
     * @param dataConfig The data configuration.
     * @param callback The callback for index updates.
     * @param paramMap The DataStore parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param config The configuration object.
     * @param executorService The executor service.
     * @param client The Dropbox client.
     * @param path The path to crawl.
     */
    protected void crawlBasicFiles(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Config config,
            final ExecutorService executorService, final DropboxClient client, final String path) {
        if (logger.isDebugEnabled()) {
            logger.debug("Crawling files.");
        }

        try {
            client.listFiles(path, false, metadata -> {
                if (metadata instanceof FileMetadata) {
                    executorService.execute(() -> {
                        storeFile(dataConfig, callback, paramMap, scriptMap, defaultDataMap, config, client, null, null, null,
                                metadata.getPathLower(), metadata, Collections.emptyList());
                    });
                } else if (metadata instanceof FolderMetadata) {
                    crawlBasicFiles(dataConfig, callback, paramMap, scriptMap, defaultDataMap, config, executorService, client,
                            metadata.getPathLower());
                } else {
                    logger.warn("Unexpected metadata: {}", metadata);
                }
            });
        } catch (DbxException e) {
            logger.warn("Failed to list files. path={}", path, e);
        }
    }

    /**
     * Crawls files for each member of a team account.
     *
     * @param dataConfig The data configuration.
     * @param callback The callback for index updates.
     * @param paramMap The DataStore parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param config The configuration object.
     * @param executorService The executor service.
     * @param client The Dropbox client.
     * @param members The list of team members.
     */
    protected void crawlMemberFiles(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Config config,
            final ExecutorService executorService, final DropboxClient client, final List<TeamMemberInfo> members) {
        if (logger.isDebugEnabled()) {
            logger.debug("Crawling member files.");
        }
        members.forEach(member -> {
            final String memberId = member.getProfile().getTeamMemberId();
            final List<String> roles = Collections.singletonList(getMemberRole(member));
            try {
                client.getMemberFiles(memberId, "", false,
                        metadata -> executorService.execute(() -> storeFile(dataConfig, callback, paramMap, scriptMap, defaultDataMap,
                                config, client, memberId, null, null,
                                "/" + member.getProfile().getName().getDisplayName() + metadata.getPathDisplay(), metadata, roles)));
            } catch (final DbxException e) {
                logger.debug("Failed to get member files.", e);
            }
        });
    }

    /**
     * Crawls files in team folders.
     *
     * @param dataConfig The data configuration.
     * @param callback The callback for index updates.
     * @param paramMap The DataStore parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param config The configuration object.
     * @param executorService The executor service.
     * @param client The Dropbox client.
     * @param members The list of team members.
     */
    protected void crawlTeamFiles(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Config config,
            final ExecutorService executorService, final DropboxClient client, final List<TeamMemberInfo> members) {
        if (logger.isDebugEnabled()) {
            logger.debug("Crawling team files.");
        }
        try {
            final String adminId = client.getAdmin(members).getProfile().getTeamMemberId();
            client.getTeamFolders(folder -> {
                final String teamFolderId = folder.getTeamFolderId();
                // TODO use group
                final List<String> roles = members.stream().map(this::getMemberRole).collect(Collectors.toList());
                try {
                    client.getTeamFiles(adminId, teamFolderId, "", false, metadata -> {
                        if (metadata instanceof FileMetadata) {
                            executorService.execute(() -> storeFile(dataConfig, callback, paramMap, scriptMap, defaultDataMap, config,
                                    client, null, adminId, teamFolderId, metadata.getPathDisplay(), metadata, roles));
                        } else if ((metadata instanceof FolderMetadata) && members.stream()
                                .noneMatch(member -> member.getProfile().getName().getDisplayName().equals(metadata.getName()))) {
                            try {
                                client.getTeamFiles(adminId, teamFolderId, metadata.getPathDisplay(), true,
                                        meta -> executorService
                                                .execute(() -> storeFile(dataConfig, callback, paramMap, scriptMap, defaultDataMap, config,
                                                        client, null, adminId, teamFolderId, meta.getPathDisplay(), meta, roles)));
                            } catch (final DbxException e) {
                                logger.debug("Failed to get team files.", e);
                            }
                        }
                    });
                } catch (final DbxException e) {
                    logger.debug("Failed to get team files.", e);
                }
            });
        } catch (final DbxException e) {
            logger.debug("Failed to get team folders.", e);
        }
    }

    /**
     * Stores a single file or folder in the index.
     *
     * @param dataConfig The data configuration.
     * @param callback The callback for index updates.
     * @param paramMap The DataStore parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param config The configuration object.
     * @param client The Dropbox client.
     * @param memberId The ID of the team member (if applicable).
     * @param adminId The ID of the administrator (if applicable).
     * @param teamFolderId The ID of the team folder (if applicable).
     * @param path The path of the file or folder.
     * @param metadata The metadata of the file or folder.
     * @param roles The roles associated with the file or folder.
     */
    protected void storeFile(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Config config, final DropboxClient client,
            final String memberId, final String adminId, final String teamFolderId, final String path, final Metadata metadata,
            final List<String> roles) {
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final StatsKeyObject statsKey = new StatsKeyObject(path);
        paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);
        try {
            crawlerStatsHelper.begin(statsKey);
            final String url = getUrl(path);

            final UrlFilter urlFilter = config.urlFilter;
            if (urlFilter != null && !urlFilter.match(url)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Not matched: {}", url);
                }
                crawlerStatsHelper.discard(statsKey);
                return;
            }

            final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());
            final Map<String, Object> fileMap = new HashMap<>();

            logger.info("Crawling URL: {}", url);

            fileMap.put(FILE_URL, url);

            // TODO permissions
            // final List<String> permissions = getFilePermissions(client, metadata);
            final List<String> permissions = new ArrayList<>(roles);
            final PermissionHelper permissionHelper = ComponentUtil.getPermissionHelper();
            StreamUtil.split(paramMap.getAsString(DEFAULT_PERMISSIONS, StringUtil.EMPTY), ",")
                    .of(stream -> stream.filter(StringUtil::isNotBlank).map(permissionHelper::encode).forEach(permissions::add));
            fileMap.put(FILE_ROLES, permissions.stream().distinct().collect(Collectors.toList()));

            fileMap.put(FILE_NAME, metadata.getName());
            fileMap.put(FILE_PATH_LOWER, metadata.getPathLower());
            fileMap.put(FILE_PATH_DISPLAY, metadata.getPathDisplay());
            fileMap.put(FILE_PARENT_SHARED_FOLDER_ID, metadata.getParentSharedFolderId());

            if (metadata instanceof FileMetadata file) {
                if (file.getSize() > config.maxSize) {
                    throw new MaxLengthExceededException(
                            "The content length (" + file.getSize() + " byte) is over " + config.maxSize + " byte. The url is " + url);
                }

                if (file.getIsDownloadable()) {
                    try (final InputStream in = (memberId != null) ? client.getMemberFileInputStream(memberId, file)
                            : adminId != null ? client.getTeamFileInputStream(adminId, teamFolderId, file)
                                    : client.getFileInputStream(file)) {
                        final String mimeType = getFileMimeType(in, file);
                        final String fileType = ComponentUtil.getFileTypeHelper().get(mimeType);
                        if (Stream.of(config.supportedMimeTypes).noneMatch(mimeType::matches)) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("{} is not an indexing target.", mimeType);
                            }
                            return;
                        }

                        fileMap.put(FILE_CONTENTS, getFileContents(in, file, mimeType, url, config.ignoreError));
                        fileMap.put(FILE_MIMETYPE, mimeType);
                        fileMap.put(FILE_FILETYPE, fileType);
                    } catch (final DbxException e) {
                        if (config.ignoreError) {
                            logger.warn("Failed to download {} by {}", file.getName(), e.getMessage());
                            return;
                        }
                        throw new DataStoreCrawlingException(url, "Failed to download " + file.getName(), e);
                    }
                } else {
                    fileMap.put(FILE_EXPORT_INFO, file.getExportInfo()); // ExportInfo
                }

                fileMap.put(FILE_ID, file.getId());
                fileMap.put(FILE_PROPERTY_GROUPS, file.getPropertyGroups()); // List<PropertyGroup>
                fileMap.put(FILE_SHARING_INFO, file.getSharingInfo()); // FileSharingInfo

                fileMap.put(FILE_CLIENT_MODIFIED, file.getClientModified());
                fileMap.put(FILE_CONTENT_HASH, file.getContentHash());
                fileMap.put(FILE_HAS_EXPLICT_SHARED_MEMBERS, file.getHasExplicitSharedMembers());
                fileMap.put(FILE_MEDIA_INFO, file.getMediaInfo()); // MediaInfo
                fileMap.put(FILE_REV, file.getRev());
                fileMap.put(FILE_SERVER_MODIFIED, file.getServerModified());
                fileMap.put(FILE_SIZE, file.getSize());
                fileMap.put(FILE_SYMLINK_INFO, file.getSymlinkInfo()); // SymlinkInfo
            } else if (metadata instanceof FolderMetadata folder) {
                if (config.ignoreFolder) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Ignore item: {}", metadata.getName());
                    }
                    return;
                }

                fileMap.put(FILE_ID, folder.getId());
                fileMap.put(FILE_PROPERTY_GROUPS, folder.getPropertyGroups()); // List<PropertyGroup>
                fileMap.put(FILE_SHARING_INFO, folder.getSharingInfo()); // FolderSharingInfo

                fileMap.put(FILE_SHARED_FOLDER_ID, folder.getSharedFolderId());
            }

            resultMap.put(FILE, fileMap);

            crawlerStatsHelper.record(statsKey, StatsAction.PREPARED);

            if (logger.isDebugEnabled()) {
                logger.debug("fileMap: {}", fileMap);
            }

            final String scriptType = getScriptType(paramMap);
            for (final Map.Entry<String, String> entry : scriptMap.entrySet()) {
                final Object convertValue = convertValue(scriptType, entry.getValue(), resultMap);
                if (convertValue != null) {
                    dataMap.put(entry.getKey(), convertValue);
                }
            }

            crawlerStatsHelper.record(statsKey, StatsAction.EVALUATED);

            if (logger.isDebugEnabled()) {
                logger.debug("dataMap: {}", dataMap);
            }

            if (dataMap.get("url") instanceof String statsUrl) {
                statsKey.setUrl(statsUrl);
            }

            callback.store(paramMap, dataMap);
            crawlerStatsHelper.record(statsKey, StatsAction.FINISHED);
        } catch (final CrawlingAccessException e) {
            logger.warn("Crawling Access Exception at : {}", dataMap, e);

            Throwable target = e;
            if (target instanceof MultipleCrawlingAccessException ex) {
                final Throwable[] causes = ex.getCauses();
                if (causes.length > 0) {
                    target = causes[causes.length - 1];
                }
            }

            String errorName;
            final Throwable cause = target.getCause();
            if (cause != null) {
                errorName = cause.getClass().getCanonicalName();
            } else {
                errorName = target.getClass().getCanonicalName();
            }

            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, errorName, "", target);
            crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
        } catch (final Throwable t) {
            logger.warn("Crawling Access Exception at : {}", dataMap, t);
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), "", t);
            crawlerStatsHelper.record(statsKey, StatsAction.EXCEPTION);
        } finally {
            crawlerStatsHelper.done(statsKey);
        }
    }

    /**
     * Generates a URL for a given path.
     *
     * @param path The path of the file or folder.
     * @return The generated URL.
     * @throws URISyntaxException If the URL syntax is invalid.
     */
    protected String getUrl(final String path) throws URISyntaxException {
        return new URIBuilder().setScheme("https").setHost("www.dropbox.com").setPath("/home" + path).build().toASCIIString();
    }

    /**
     * Guesses the MIME type of a file.
     *
     * @param in The input stream of the file.
     * @param file The file metadata.
     * @return The guessed MIME type.
     */
    protected String getFileMimeType(final InputStream in, final FileMetadata file) {
        try {
            final String mimeType = URLConnection.guessContentTypeFromStream(in);
            if (mimeType != null) {
                return mimeType;
            }
        } catch (final IOException e) {
            logger.warn("Failed to get file mime type: {}", file.getName(), e);
        }
        final String mimeType = URLConnection.guessContentTypeFromName(file.getName());
        return (mimeType != null) ? mimeType : "application/octet-stream";
    }

    /**
     * Extracts the content of a file.
     *
     * @param in The input stream of the file.
     * @param file The file metadata.
     * @param mimeType The MIME type of the file.
     * @param url The URL of the file.
     * @param ignoreError Whether to ignore errors during content extraction.
     * @return The extracted content.
     */
    protected String getFileContents(final InputStream in, final FileMetadata file, final String mimeType, final String url,
            final boolean ignoreError) {
        try {
            return ComponentUtil.getExtractorFactory().builder(in, null).mimeType(mimeType).extractorName(extractorName).extract()
                    .getContent();
        } catch (final Exception e) {
            if (!ignoreError && !ComponentUtil.getFessConfig().isCrawlerIgnoreContentException()) {
                throw new DataStoreCrawlingException(url, "Failed to get contents: " + file.getName(), e);
            }
            if (logger.isDebugEnabled()) {
                logger.warn("Failed to get contents: {}", file.getName(), e);
            } else {
                logger.warn("Failed to get contents: {}. {}", file.getName(), e.getMessage());
            }
            return StringUtil.EMPTY;
        }
    }

    /**
     * Gets the role of a team member.
     *
     * @param member The team member information.
     * @return The role of the team member.
     */
    protected String getMemberRole(final TeamMemberInfo member) {
        return ComponentUtil.getSystemHelper().getSearchRoleByUser(member.getProfile().getEmail());
    }

    /**
     * Creates a new Dropbox client.
     *
     * @param paramMap The DataStore parameters.
     * @return A new Dropbox client.
     */
    protected DropboxClient createClient(final DataStoreParams paramMap) {
        return new DropboxClient(paramMap);
    }

    /**
     * Configuration class for the DropboxDataStore.
     */
    protected static class Config {
        final String[] fields;
        final long maxSize;
        final boolean ignoreFolder, ignoreError;
        final String[] supportedMimeTypes;
        final UrlFilter urlFilter;

        /**
         * Constructs a new Config object from the given DataStore parameters.
         *
         * @param paramMap The DataStore parameters.
         */
        Config(final DataStoreParams paramMap) {
            fields = getFields(paramMap);
            maxSize = getMaxSize(paramMap);
            ignoreFolder = isIgnoreFolder(paramMap);
            ignoreError = isIgnoreError(paramMap);
            supportedMimeTypes = getSupportedMimeTypes(paramMap);
            urlFilter = getUrlFilter(paramMap);
        }

        private String[] getFields(final DataStoreParams paramMap) {
            final String value = paramMap.getAsString(FIELDS);
            if (value != null) {
                return StreamUtil.split(value, ",").get(stream -> stream.map(String::trim).toArray(String[]::new));
            }
            return null;
        }

        private long getMaxSize(final DataStoreParams paramMap) {
            final String value = paramMap.getAsString(MAX_SIZE);
            try {
                return StringUtil.isNotBlank(value) ? Long.parseLong(value) : DEFAULT_MAX_SIZE;
            } catch (final NumberFormatException e) {
                return DEFAULT_MAX_SIZE;
            }
        }

        private boolean isIgnoreFolder(final DataStoreParams paramMap) {
            return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_FOLDER, Constants.TRUE));
        }

        private boolean isIgnoreError(final DataStoreParams paramMap) {
            return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_ERROR, Constants.TRUE));
        }

        private String[] getSupportedMimeTypes(final DataStoreParams paramMap) {
            return StreamUtil.split(paramMap.getAsString(SUPPORTED_MIMETYPES, ".*"), ",")
                    .get(stream -> stream.map(String::trim).toArray(String[]::new));
        }

        private UrlFilter getUrlFilter(final DataStoreParams paramMap) {
            final UrlFilter urlFilter;
            try {
                urlFilter = ComponentUtil.getComponent(UrlFilter.class);
            } catch (final ComponentNotFoundException e) {
                return null;
            }
            final String include = paramMap.getAsString(INCLUDE_PATTERN);
            if (StringUtil.isNotBlank(include)) {
                urlFilter.addInclude(include);
            }
            final String exclude = paramMap.getAsString(EXCLUDE_PATTERN);
            if (StringUtil.isNotBlank(exclude)) {
                urlFilter.addExclude(exclude);
            }
            urlFilter.init(paramMap.getAsString(Constants.CRAWLING_INFO_ID));
            if (logger.isDebugEnabled()) {
                logger.debug("urlFilter: {}", urlFilter);
            }
            return urlFilter;
        }

        @Override
        public String toString() {
            return "{fields=" + Arrays.toString(fields) + ",maxSize=" + maxSize + ",ignoreError=" + ignoreError + ",ignoreFolder="
                    + ignoreFolder + ",supportedMimeTypes=" + Arrays.toString(supportedMimeTypes) + ",urlFilter=" + urlFilter + "}";
        }
    }

}
