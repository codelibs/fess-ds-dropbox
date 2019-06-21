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
import com.dropbox.core.v2.files.Metadata;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.lastaflute.di.core.exception.ComponentNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
