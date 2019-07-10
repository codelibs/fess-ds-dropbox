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
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.codelibs.fess.ds.dropbox.DropboxDataStore.NUMBER_OF_THREADS;

public class DropboxPaperDataStore extends AbstractDataStore {

    private static final Logger logger = LoggerFactory.getLogger(DropboxPaperDataStore.class);

    // scripts
    protected static final String PAPER = "paper";
    protected static final String PAPER_URL = "url";

    // other
    protected String extractorName = "tikaExtractor";

    protected String getName() {
        return "Dropbox(Paper)";
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {
        final ExecutorService executorService =
                Executors.newFixedThreadPool(Integer.parseInt(paramMap.getOrDefault(NUMBER_OF_THREADS, "1")));
        try {
            final DropboxClient client = createClient(paramMap);
            // TODO crawlMemberPapers
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
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final DropboxClient client) {
        if (logger.isDebugEnabled()) {
            logger.debug("Crawling user papers.");
        }
        try {
            client.getMembers(member -> {
                final String memberId = member.getProfile().getTeamMemberId();
                try {
                    client.getMemberPaperIds(memberId, docId -> {
                        // client.getPaperDownloader(memberId, docId)
                    });
                } catch (final DbxException e) {
                    logger.debug("Failed to crawl member papers: {}", memberId, e);
                }
            });
        } catch (final DbxException e) {
            logger.debug("Failed to crawl member papers.", e);
        }
    }

    protected void storeFile(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final DropboxClient client) {
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        try {
            final String url = ""; // TODO

            final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap);
            final Map<String, Object> paperMap = new HashMap<>();

            logger.info("Crawling URL: {}", url);

            paperMap.put(PAPER_URL, url);

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

    protected DropboxClient createClient(final Map<String, String> paramMap) {
        return new DropboxClient(paramMap);
    }

}
