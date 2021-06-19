Dropbox Data Store for Fess
[![Java CI with Maven](https://github.com/codelibs/fess-ds-dropbox/actions/workflows/maven.yml/badge.svg)](https://github.com/codelibs/fess-ds-dropbox/actions/workflows/maven.yml)
==========================

## Overview

Dropbox Data Store is an extension for Fess Data Store Crawling.

## Download

See [Maven Repository](http://central.maven.org/maven2/org/codelibs/fess/fess-ds-dropbox/).

## Installation

See [Plugin](https://fess.codelibs.org/13.4/admin/plugin-guide.html) of Administration guide.

## Getting Started

### Parameters

```properties
access_token=**********
```

| Key | Value |
| --- | --- |
| *access_token* | An access token of Dropbox (Generated on [App Console](https://www.dropbox.com/developers/apps)) |

### Scripts (Dropbox)

```properties
url=file.url
title=file.name
content=file.contents
mimetype=file.mimetype
filetype=file.filetype
filename=file.name
content_length=file.size
last_modified=file.client_modified
role=paper.roles
```

| Key | Value |
| --- | --- |
| *file.url* | The preview link of the file. |
| *file.contents* | The text contents of the file. |
| *file.mimetype* | The MIME type of the file. |
| *file.filetype* | The file type ot the file. |
| *file.name* | The name of the file. |
| *file.path_display* | The path of the file. |
| *file.size* | The size of the file. |
| *file.client_modified* | The last time the file was modified. (client) |
| *file.server_modified* | The last time the file was modified. (server) |

### Scripts (DropboxPaper)

```properties
title=paper.title
content=paper.contents
url=paper.url
mimetype=paper.mimetype
filetype=paper.filetype
role=paper.roles
```

| Key | Value |
| --- | --- |
| *paper.url* | The preview link of the paper. |
| *paper.contents* | The text contents of the paper. |
| *paper.mimetype* | The MIME type of the paper. |
| *paper.filetype* | The file type ot the paper. |
| *paper.title* | The title of the paper. |
| *paper.owner* | The owner of the paper. |

### Roles

Add parameter `default_permissions` and script `role`.

```properties
default_permissions={role}admin
```

```properties
role=file.roles
```

```properties
role=paper.roles
```
