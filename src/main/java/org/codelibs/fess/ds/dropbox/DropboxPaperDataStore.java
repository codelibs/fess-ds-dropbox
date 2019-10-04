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

import com.dropbox.core.DbxDownloader;
import com.dropbox.core.DbxException;
import com.dropbox.core.v2.files.Metadata;
import com.dropbox.core.v2.paper.PaperDocExportResult;
import com.dropbox.core.v2.team.TeamMemberInfo;
import org.apache.http.client.utils.URIBuilder;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.crawler.extractor.Extractor;
import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.dropbox.DropboxDataStore.Config;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.helper.PermissionHelper;
import org.codelibs.fess.util.ComponentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.codelibs.fess.ds.dropbox.DropboxDataStore.DEFAULT_PERMISSIONS;
import static org.codelibs.fess.ds.dropbox.DropboxDataStore.NUMBER_OF_THREADS;

public class DropboxPaperDataStore extends AbstractDataStore {

    private static final Logger logger = LoggerFactory.getLogger(DropboxPaperDataStore.class);

    // scripts
    protected static final String PAPER = "paper";
    protected static final String PAPER_URL = "url";
    protected static final String PAPER_TITLE = "title";
    protected static final String PAPER_CONTENTS = "contents";
    protected static final String PAPER_OWNER = "owner";
    protected static final String PAPER_MIMETYPE = "mimetype";
    protected static final String PAPER_FILETYPE = "filetype";
    protected static final String PAPER_REVISION = "revision";
    protected static final String PAPER_ROLES = "roles";

    // other
    protected String extractorName = "tikaExtractor";

    protected String getName() {
        return this.getClass().getSimpleName();
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
            crawlMemberPapers(dataConfig, callback, paramMap, scriptMap, defaultDataMap, executorService, config, client);
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Interrupted.", e);
            }
        } finally {
            executorService.shutdown();
        }
    }

    protected void crawlMemberPapers(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final ExecutorService executorService,
            final Config config, final DropboxClient client) {
        if (logger.isDebugEnabled()) {
            logger.debug("Crawling member papers.");
        }
        try {
            client.getMembers(member -> {
                final String memberId = member.getProfile().getTeamMemberId();
                final List<String> roles = Collections.singletonList(getMemberRole(member));
                try {
                    client.getMemberPaperIds(memberId, docId -> executorService.execute(
                            () -> storePaper(dataConfig, callback, paramMap, scriptMap, defaultDataMap, config, client, memberId, docId,
                                    roles)));
                    client.getMemberFiles(memberId, "", true, metadata -> executorService.execute(
                            () -> storePaperFile(dataConfig, callback, paramMap, scriptMap, defaultDataMap, config, client, memberId, null, null,
                                    "/" + member.getProfile().getName().getDisplayName() + metadata.getPathDisplay(), metadata, roles)));
                } catch (final DbxException e) {
                    logger.debug("Failed to crawl member papers: {}", memberId, e);
                }
            });
        } catch (final DbxException e) {
            logger.debug("Failed to crawl member papers.", e);
        }
    }

    protected void storePaper(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Config config, final DropboxClient client,
            final String memberId, final String docId, final List<String> roles) {
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        try {
            final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap);
            final Map<String, Object> paperMap = new HashMap<>();

            final String url = getUrlFromId(docId);

            final UrlFilter urlFilter = config.urlFilter;
            if (urlFilter != null && !urlFilter.match(url)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Not matched: {}", url);
                }
                return;
            }

            logger.info("Crawling URL: {}", url);

            paperMap.put(PAPER_URL, url);

            final DbxDownloader<PaperDocExportResult> downloader = client.getPaperDownloader(memberId, docId);
            final PaperDocExportResult result = downloader.getResult();
            paperMap.put(PAPER_TITLE, result.getTitle());
            final String mimeType = result.getMimeType();
            final String fileType = ComponentUtil.getFileTypeHelper().get(mimeType);
            paperMap.put(PAPER_CONTENTS, getPaperContents(downloader.getInputStream(), mimeType, url, config.ignoreError));
            paperMap.put(PAPER_OWNER, result.getOwner());
            paperMap.put(PAPER_MIMETYPE, mimeType);
            paperMap.put(PAPER_FILETYPE, fileType);
            paperMap.put(PAPER_REVISION, result.getRevision());

            // TODO permissions
            // final List<String> permissions = getPaperPermissions(client, memberId, docId);
            final List<String> permissions = new ArrayList<>();
            permissions.addAll(roles);
            final PermissionHelper permissionHelper = ComponentUtil.getPermissionHelper();
            StreamUtil.split(paramMap.get(DEFAULT_PERMISSIONS), ",")
                    .of(stream -> stream.filter(StringUtil::isNotBlank).map(permissionHelper::encode).forEach(permissions::add));
            paperMap.put(PAPER_ROLES, permissions.stream().distinct().collect(Collectors.toList()));

            resultMap.put(PAPER, paperMap);

            if (logger.isDebugEnabled()) {
                logger.debug("paperMap: {}", paperMap);
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

    protected void storePaperFile(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
                             final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Config config, final DropboxClient client,
                             final String memberId, final String adminId, final String teamFolderId, final String path, final Metadata metadata,
                             final List<String> roles) {
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        try {
            final String url = getUrlFromPath(path);

            final UrlFilter urlFilter = config.urlFilter;
            if (urlFilter != null && !urlFilter.match(url)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Not matched: {}", url);
                }
                return;
            }

            final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap);
            final Map<String, Object> paperMap = new HashMap<>();

            logger.info("Crawling URL: {}", url);

            paperMap.put(PAPER_URL, url);
            paperMap.put(PAPER_TITLE, metadata.getName());
            // cannot download Paper Documents as Markdown on Dropbox because the API does not support yet.
            paperMap.put(PAPER_CONTENTS, metadata.getName());

            // TODO permissions
            // final List<String> permissions = getFilePermissions(client, metadata);
            final List<String> permissions = new ArrayList<>();
            permissions.addAll(roles);
            final PermissionHelper permissionHelper = ComponentUtil.getPermissionHelper();
            StreamUtil.split(paramMap.get(DEFAULT_PERMISSIONS), ",")
                    .of(stream -> stream.filter(StringUtil::isNotBlank).map(permissionHelper::encode).forEach(permissions::add));
            paperMap.put(PAPER_ROLES, permissions.stream().distinct().collect(Collectors.toList()));


            resultMap.put(PAPER, paperMap);

            if (logger.isDebugEnabled()) {
                logger.debug("paperMap: {}", paperMap);
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

    protected String getUrlFromId(final String docId) throws URISyntaxException {
        return new URIBuilder().setScheme("https").setHost("paper.dropbox.com").setPath("/doc/" + docId).build().toASCIIString();
    }

    protected String getUrlFromPath(final String path) throws URISyntaxException {
        return new URIBuilder().setScheme("https").setHost("www.dropbox.com").setPath("/home" + path).build().toASCIIString();
    }

    protected String getPaperContents(final InputStream in, final String mimeType, final String url, final boolean ignoreError) {
        try {
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
                logger.warn("Failed to get paper contents: " + url, e);
                return StringUtil.EMPTY;
            } else {
                throw new DataStoreCrawlingException(url, "Failed to get paper contents", e);
            }
        }
    }

    protected String getMemberRole(final TeamMemberInfo member) {
        return ComponentUtil.getSystemHelper().getSearchRoleByUser(member.getProfile().getEmail());
    }

    protected DropboxClient createClient(final Map<String, String> paramMap) {
        return new DropboxClient(paramMap);
    }

}
