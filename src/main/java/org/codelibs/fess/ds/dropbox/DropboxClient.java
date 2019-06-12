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
import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.v1.DbxEntry;
import com.dropbox.core.v2.DbxTeamClientV2;
import com.dropbox.core.v2.team.TeamMemberInfo;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.exception.DataStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class DropboxClient {

    private static final Logger logger = LoggerFactory.getLogger(DropboxClient.class);

    protected static final String APP_KEY = "app_key";
    protected static final String APP_SECRET = "app_secret";
    protected static final String ACCESS_TOKEN = "access_token";

    protected DbxRequestConfig config;
    protected DbxTeamClientV2 client;
    protected Map<String, String> params;

    public DropboxClient(final Map<String, String> params) {
        this.params = params;

        final String accessToken = params.getOrDefault(ACCESS_TOKEN, StringUtil.EMPTY);
        if (StringUtil.isBlank(accessToken)) {
            throw new DataStoreException("Parameter '" + ACCESS_TOKEN + "' is required");
        }

        this.config = new DbxRequestConfig("fess");
        this.client = new DbxTeamClientV2(config, accessToken);
    }

    public void getMembers(final Consumer<TeamMemberInfo> consumer) throws DbxException {
        client.team().membersList().getMembers().forEach(consumer);
    }

    // TODO get files, download file

    public Map getList(){
        final Map<String,String> fileList=new HashMap<>();
        //DbxEntry.WithChildren listing =

        return fileList;
    }
}
