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
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.FolderMetadata;
import com.dropbox.core.v2.files.Metadata;
import org.apache.commons.io.IOUtils;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MaxLengthExceededException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.crawler.extractor.Extractor;
import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.util.ComponentUtil;
import org.lastaflute.di.core.exception.ComponentNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class DropboxDataStore extends AbstractDataStore {

    private static final Logger logger = LoggerFactory.getLogger(DropboxDataStore.class);

    protected static final long DEFAULT_MAX_SIZE = 10000000L; // 10m

    // parameters
    protected static final String FIELDS = "fields";
    protected static final String MAX_SIZE = "max_size";
    protected static final String IGNORE_FOLDER = "ignore_folder";
    protected static final String IGNORE_ERROR = "ignore_error";
    protected static final String SUPPORTED_MIMETYPES = "supported_mimetypes";
    protected static final String INCLUDE_PATTERN = "include_pattern";
    protected static final String EXCLUDE_PATTERN = "exclude_pattern";
    protected static final String NUMBER_OF_THREADS = "number_of_threads";

    // scripts
    protected static final String FILE = "file";

    // - common
    protected static final String FILE_URL = "url";

    protected static final String FILE_NAME = "name";
    protected static final String FILE_PATH_LOWER = "path_lower";
    protected static final String FILE_PATH_DISPLAY = "path_display";
    protected static final String FILE_PARENT_SHARED_FOLDER_ID = "parent_shared_folder_id";

    protected static final String FILE_ID = "id";
    protected static final String FILE_PROPERTY_GROUPS = "property_groups";
    protected static final String FILE_SHARING_INFO = "sharing_info";

    // - file
    protected static final String FILE_CONTENTS = "contents";
    protected static final String FILE_MIMETYPE = "mimetype";
    protected static final String FILE_FILETYPE = "filetype";

    protected static final String FILE_CLIENT_MODIFIED = "client_modified";
    protected static final String FILE_CONTENT_HASH = "content_hash";
    protected static final String FILE_HAS_EXPLICT_SHARED_MEMBERS = "has_explict_shared_members";
    protected static final String FILE_MEDIA_INFO = "media_info";
    protected static final String FILE_REV = "rev";
    protected static final String FILE_SERVER_MODIFIED = "server_modified";
    protected static final String FILE_SIZE = "size";
    protected static final String FILE_SYMLINK_INFO = "symlink_info";

    // - folder
    protected static final String FILE_SHARED_FOLDER_ID = "shared_folder_id";

    // other
    protected String extractorName = "tikaExtractor";

    protected String getName() {
        return "Dropbox";
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {
        final Config config = new Config(paramMap);
        if (logger.isDebugEnabled()) {
            logger.debug("config: {}", config);
        }
        final ExecutorService executorService =
                Executors.newFixedThreadPool(Integer.parseInt(paramMap.getOrDefault(NUMBER_OF_THREADS, "1")));

        try {
            final DropboxClient client = createClient(paramMap);
            crawlMemberFiles(dataConfig, callback, paramMap, scriptMap, defaultDataMap, config, executorService, client);
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Interrupted.", e);
            }
        } finally {
            executorService.shutdown();
        }
    }

    protected void crawlMemberFiles(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Config config,
            final ExecutorService executorService, final DropboxClient client) {
        if (logger.isDebugEnabled()) {
            logger.debug("Crawling member files.");
        }
        try {
            client.getMembers(member -> {
                // TODO implement Client
                // client.getMemberFiles(member.getProfile().getAccountId(), metadata -> executorService
                //         .execute(() -> storeFile(dataConfig, callback, paramMap, scriptMap, defaultDataMap, config, client, metadata)));
            });
        } catch (final DbxException e) {
            logger.debug("Failed to crawl member files.", e);
        }
    }

    protected void storeFile(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Config config, final DropboxClient client,
            final Metadata metadata) {
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        try {
            final String url = getUrl(client, metadata);

            final UrlFilter urlFilter = config.urlFilter;
            if (urlFilter != null && !urlFilter.match(url)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Not matched: {}", url);
                }
                return;
            }

            if (config.ignoreFolder && metadata instanceof FolderMetadata) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Ignore item: {}", metadata.getName());
                }
                return;
            }

            if (metadata instanceof FileMetadata) {
                final FileMetadata file = (FileMetadata) metadata;
                final String mimeType = getFileMimeType(client, file);
                if (Stream.of(config.supportedMimeTypes).noneMatch(mimeType::matches)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("{} is not an indexing target.", mimeType);
                    }
                    return;
                }
                if (file.getSize() > config.maxSize) {
                    throw new MaxLengthExceededException(
                            "The content length (" + file.getSize() + " byte) is over " + config.maxSize + " byte. The url is " + url);
                }
            }

            logger.info("Crawling URL: {}", url);

            final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap);
            final Map<String, Object> fileMap = new HashMap<>();

            fileMap.put(FILE_URL, url);
            fileMap.put(FILE_NAME, metadata.getName());
            fileMap.put(FILE_PATH_LOWER, metadata.getPathLower());
            fileMap.put(FILE_PATH_DISPLAY, metadata.getPathDisplay());
            fileMap.put(FILE_PARENT_SHARED_FOLDER_ID, metadata.getParentSharedFolderId());

            if (metadata instanceof FileMetadata) {
                final FileMetadata file = (FileMetadata) metadata;

                fileMap.put(FILE_ID, file.getId());
                fileMap.put(FILE_PROPERTY_GROUPS, file.getPropertyGroups()); // List<PropertyGroup>
                fileMap.put(FILE_SHARING_INFO, file.getSharingInfo()); // FileSharingInfo

                final String mimeType = getFileMimeType(client, file);
                final String fileType = ComponentUtil.getFileTypeHelper().get(mimeType);
                fileMap.put(FILE_CONTENTS, getFileContents(client, file, config.ignoreError));
                fileMap.put(FILE_MIMETYPE, mimeType);
                fileMap.put(FILE_FILETYPE, fileType);

                fileMap.put(FILE_CLIENT_MODIFIED, file.getClientModified());
                fileMap.put(FILE_CONTENT_HASH, file.getContentHash());
                fileMap.put(FILE_HAS_EXPLICT_SHARED_MEMBERS, file.getHasExplicitSharedMembers());
                fileMap.put(FILE_MEDIA_INFO, file.getMediaInfo()); // MediaInfo
                fileMap.put(FILE_REV, file.getRev());
                fileMap.put(FILE_SERVER_MODIFIED, file.getServerModified());
                fileMap.put(FILE_SIZE, file.getSize());
                fileMap.put(FILE_SYMLINK_INFO, file.getSymlinkInfo()); // SymlinkInfo
            } else if (metadata instanceof FolderMetadata) {
                final FolderMetadata folder = (FolderMetadata) metadata;

                fileMap.put(FILE_ID, folder.getId());
                fileMap.put(FILE_PROPERTY_GROUPS, folder.getPropertyGroups()); // List<PropertyGroup>
                fileMap.put(FILE_SHARING_INFO, folder.getSharingInfo()); // FolderSharingInfo

                fileMap.put(FILE_SHARED_FOLDER_ID, folder.getSharedFolderId());
            }

            resultMap.put(FILE, fileMap);

            if (logger.isDebugEnabled()) {
                logger.debug("fileMap: {}", fileMap);
            }

            for (final Map.Entry<String, String> entry : scriptMap.entrySet()) {
                final Object convertValue = convertValue(entry.getValue(), resultMap);
                if (convertValue != null) {
                    dataMap.put(entry.getKey(), convertValue);
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("dataMap: {}", dataMap);
            }

            callback.store(paramMap, dataMap);
        } catch (final CrawlingAccessException e) {
            logger.warn("Crawling Access Exception at : " + dataMap, e);

            Throwable target = e;
            if (target instanceof MultipleCrawlingAccessException) {
                final Throwable[] causes = ((MultipleCrawlingAccessException) target).getCauses();
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
        } catch (final Throwable t) {
            logger.warn("Crawling Access Exception at : " + dataMap, t);
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), "", t);
        }
    }

    protected String getUrl(final DropboxClient client, final Metadata metadata) {
        // TODO implement
        return metadata.getPathLower();
    }

    protected String getFileMimeType(final DropboxClient client, final FileMetadata file) {
        // TODO implement
        return file.getMediaInfo().toString();
    }

    protected String getFileContents(final DropboxClient client, final FileMetadata file, final boolean ignoreError) {
        // TODO implement Client
        // try (final InputStream in = client.getFileInputStream(file)) {
        try (final InputStream in = IOUtils.toInputStream(file.getName(), StandardCharsets.UTF_8)) {
            final String mimeType = getFileMimeType(client, file);
            Extractor extractor = ComponentUtil.getExtractorFactory().getExtractor(mimeType);
            if (extractor == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("use a default extractor as {} by {}", extractorName, mimeType);
                }
                extractor = ComponentUtil.getComponent(extractorName);
            }
            return extractor.getText(in, null).getContent();
        } catch (final Exception e) {
            if (ignoreError) {
                logger.warn("Failed to get contents: " + file.getName(), e);
                return StringUtil.EMPTY;
            } else {
                throw new DataStoreCrawlingException(getUrl(client, file), "Failed to get contents: " + file.getName(), e);
            }
        }
    }

    protected DropboxClient createClient(final Map<String, String> paramMap) {
        return new DropboxClient(paramMap);
    }

    protected static class Config {
        final String[] fields;
        final long maxSize;
        final boolean ignoreFolder, ignoreError;
        final String[] supportedMimeTypes;
        final UrlFilter urlFilter;

        Config(final Map<String, String> paramMap) {
            fields = getFields(paramMap);
            maxSize = getMaxSize(paramMap);
            ignoreFolder = isIgnoreFolder(paramMap);
            ignoreError = isIgnoreError(paramMap);
            supportedMimeTypes = getSupportedMimeTypes(paramMap);
            urlFilter = getUrlFilter(paramMap);
        }

        private String[] getFields(final Map<String, String> paramMap) {
            final String value = paramMap.get(FIELDS);
            if (value != null) {
                return StreamUtil.split(value, ",").get(stream -> stream.map(String::trim).toArray(String[]::new));
            }
            return null;
        }

        private long getMaxSize(final Map<String, String> paramMap) {
            final String value = paramMap.get(MAX_SIZE);
            try {
                return StringUtil.isNotBlank(value) ? Long.parseLong(value) : DEFAULT_MAX_SIZE;
            } catch (final NumberFormatException e) {
                return DEFAULT_MAX_SIZE;
            }
        }

        private boolean isIgnoreFolder(final Map<String, String> paramMap) {
            return paramMap.getOrDefault(IGNORE_FOLDER, Constants.TRUE).equalsIgnoreCase(Constants.TRUE);
        }

        private boolean isIgnoreError(final Map<String, String> paramMap) {
            return paramMap.getOrDefault(IGNORE_ERROR, Constants.TRUE).equalsIgnoreCase(Constants.TRUE);
        }

        private String[] getSupportedMimeTypes(final Map<String, String> paramMap) {
            return StreamUtil.split(paramMap.getOrDefault(SUPPORTED_MIMETYPES, ".*"), ",")
                    .get(stream -> stream.map(String::trim).toArray(String[]::new));
        }

        private UrlFilter getUrlFilter(final Map<String, String> paramMap) {
            final UrlFilter urlFilter;
            try {
                urlFilter = ComponentUtil.getComponent(UrlFilter.class);
            } catch (final ComponentNotFoundException e) {
                return null;
            }
            final String include = paramMap.get(INCLUDE_PATTERN);
            if (StringUtil.isNotBlank(include)) {
                urlFilter.addInclude(include);
            }
            final String exclude = paramMap.get(EXCLUDE_PATTERN);
            if (StringUtil.isNotBlank(exclude)) {
                urlFilter.addExclude(exclude);
            }
            urlFilter.init(paramMap.get(Constants.CRAWLING_INFO_ID));
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
