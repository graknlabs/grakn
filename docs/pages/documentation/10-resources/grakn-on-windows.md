---
title: Using GRAKN.AI on Windows
keywords: setup, getting started, download
last_updated: December 2016
tags: [getting-started]
summary: "A guide to setting up GRAKN.AI on Windows."
sidebar: documentation_sidebar
permalink: /documentation/resources/grakn-on-windows.html
folder: documentation
---

{% include warning.html content="**Please note that there is no official support for Windows, although we expect it to be added later to GRAKN.AI. This is a suggested workaround.**" %}

## Windows Support

GRAKN is currently not supported natively on Windows. This is due to beta status of [Cassandra 2.1 on Windows](https://issues.apache.org/jira/browse/CASSANDRA-10673).

Your current options are:
- a virtual machine with Ubuntu (eg. [VirtualBox](https://www.virtualbox.org/wiki/Downloads)) 
- [Docker for Windows](https://docs.docker.com/docker-for-windows/) with our [Docker image](https://hub.docker.com/r/graknlabs/grakn/)

{% include links.html %}
