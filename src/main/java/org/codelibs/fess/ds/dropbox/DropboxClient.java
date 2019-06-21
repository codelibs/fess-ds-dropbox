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
import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.v2.DbxTeamClientV2;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.ListFolderResult;
import com.dropbox.core.v2.files.Metadata;
import com.dropbox.core.v2.team.TeamMemberInfo;
import org.apache.commons.io.output.DeferredFileOutputStream;
import org.apache.commons.lang3.SystemUtils;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.util.TemporaryFileInputStream;
import org.codelibs.fess.exception.DataStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.function.Consumer;

public class DropboxClient {

    private static final Logger logger = LoggerFactory.getLogger(DropboxClient.class);

    protected static final String APP_KEY = "app_key";
    protected static final String APP_SECRET = "app_secret";
    protected static final String ACCESS_TOKEN = "access_token";

    protected static final String MAX_CACHED_CONTENT_SIZE = "max_cached_content_size";

    protected DbxRequestConfig config;
    protected DbxTeamClientV2 client;
    protected Map<String, String> params;

    protected int maxCachedContentSize = 1024 * 1024;

    public DropboxClient(final Map<String, String> params) {
        this.params = params;

        final String accessToken = params.getOrDefault(ACCESS_TOKEN, StringUtil.EMPTY);
        if (StringUtil.isBlank(accessToken)) {
            throw new DataStoreException("Parameter '" + ACCESS_TOKEN + "' is required");
        }

        this.config = new DbxRequestConfig("fess");
        this.client = new DbxTeamClientV2(config, accessToken);

        final String size = params.get(MAX_CACHED_CONTENT_SIZE);
        if (StringUtil.isNotBlank(size)) {
            maxCachedContentSize = Integer.parseInt(size);
        }
    }

    public void getMembers(final Consumer<TeamMemberInfo> consumer) throws DbxException {
        client.team().membersList().getMembers().forEach(consumer);
    }

    public void getMemberFiles(final String memberId, final String path, final Consumer<Metadata> consumer) throws DbxException {
        ListFolderResult listFolderResult = client.asMember(memberId).files().listFolderBuilder(path).withRecursive(true).start();
        while (true) {
            listFolderResult.getEntries().forEach(consumer);
            if (!listFolderResult.getHasMore()) {
                break;
            }
            listFolderResult = client.asMember(memberId).files().listFolderContinue(listFolderResult.getCursor());
        }
    }

    public DbxDownloader<FileMetadata> getFileDownloader(final String memberId, final FileMetadata file) throws DbxException {
        return client.asMember(memberId).files().download(file.getPathDisplay());
    }

    public InputStream getFileInputStream(final DbxDownloader<FileMetadata> downloader, final FileMetadata file) {
        try (final DeferredFileOutputStream dfos = new DeferredFileOutputStream(maxCachedContentSize, "crawler-DropboxClient-", ".out",
                SystemUtils.getJavaIoTmpDir())) {
            downloader.download(dfos);
            dfos.flush();
            if (dfos.isInMemory()) {
                return new ByteArrayInputStream(dfos.getData());
            } else {
                return new TemporaryFileInputStream(dfos.getFile());
            }
        } catch (final Exception e) {
            throw new CrawlingAccessException("Failed to create an input stream from " + file.getId(), e);
        }
    }

}
