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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.dropbox.core.http.StandardHttpRequestor;

/**
 * Tests for the {@code proxy_host} / {@code proxy_port} parameters. The "proxy" is a loopback
 * stub that records the first request line of every connection and answers 502, so nothing leaves
 * the machine as long as the proxy parameters are honoured.
 */
public class DropboxClientProxyTest {

    private ServerSocket proxy;
    private final List<String> requestLines = new CopyOnWriteArrayList<>();

    @BeforeEach
    void startProxy() throws IOException {
        proxy = new ServerSocket(0, 5, InetAddress.getLoopbackAddress());
        final Thread thread = new Thread(() -> {
            while (!proxy.isClosed()) {
                try (Socket socket = proxy.accept()) {
                    final BufferedReader in =
                            new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.ISO_8859_1));
                    requestLines.add(in.readLine());
                    final OutputStream out = socket.getOutputStream();
                    out.write("HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .getBytes(StandardCharsets.ISO_8859_1));
                    out.flush();
                } catch (final IOException e) {
                    // closed by the test, or the client went away
                }
            }
        }, "dropbox-proxy-stub");
        thread.setDaemon(true);
        thread.start();
    }

    @AfterEach
    void stopProxy() throws IOException {
        proxy.close();
    }

    private DataStoreParams params() {
        final DataStoreParams params = new DataStoreParams();
        params.put(DropboxClient.ACCESS_TOKEN, "test_token_123");
        return params;
    }

    private DataStoreParams proxyParams() {
        final DataStoreParams params = params();
        params.put(DropboxClient.PROXY_HOST, "127.0.0.1");
        params.put(DropboxClient.PROXY_PORT, String.valueOf(proxy.getLocalPort()));
        return params;
    }

    @Test
    void basicClientConnectsThroughTheProxy() {
        final DropboxClient client = new DropboxClient(proxyParams());
        assertThrows(Exception.class, () -> client.basicClient.users().getCurrentAccount());
        assertEquals(List.of("CONNECT api.dropboxapi.com:443 HTTP/1.1"), requestLines);
    }

    @Test
    void teamClientConnectsThroughTheProxy() {
        final DropboxClient client = new DropboxClient(proxyParams());
        assertThrows(Exception.class, () -> client.teamClient.team().getInfo());
        assertEquals(List.of("CONNECT api.dropboxapi.com:443 HTTP/1.1"), requestLines);
    }

    @Test
    void noProxyParametersKeepTheDefaultRequestor() {
        final DropboxClient client = new DropboxClient(params());
        assertSame(StandardHttpRequestor.INSTANCE, client.config.getHttpRequestor());
    }

    @Test
    void blankProxyHostKeepsTheDefaultRequestor() {
        final DataStoreParams params = params();
        params.put(DropboxClient.PROXY_HOST, " ");
        params.put(DropboxClient.PROXY_PORT, "8080");
        assertSame(StandardHttpRequestor.INSTANCE, new DropboxClient(params).config.getHttpRequestor());
    }

    @Test
    void proxyHostWithoutPortIsRejected() {
        final DataStoreParams params = params();
        params.put(DropboxClient.PROXY_HOST, "127.0.0.1");
        final DataStoreException e = assertThrows(DataStoreException.class, () -> new DropboxClient(params));
        assertTrue(e.getMessage().contains(DropboxClient.PROXY_PORT));
    }

    @Test
    void invalidProxyPortIsRejected() {
        final DataStoreParams params = params();
        params.put(DropboxClient.PROXY_HOST, "127.0.0.1");
        params.put(DropboxClient.PROXY_PORT, "abc");
        final DataStoreException e = assertThrows(DataStoreException.class, () -> new DropboxClient(params));
        assertTrue(e.getMessage().contains(DropboxClient.PROXY_PORT));
    }
}
