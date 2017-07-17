---
title: Task Client API
keywords: java
last_updated: June 2017
tags: [java]
summary: "The Task Client API"
sidebar: documentation_sidebar
permalink: /documentation/developing-with-java/task-api.html
folder: documentation
---


The Task client is a Java api for interacting with the Grakn engine tasks api. To use the tasks client API, add the following to your pom.xml:

```
<dependency>
	<groupId>ai.grakn</groupId>
	<artifactId>grakn-client</artifactId>
	<version>${grakn.version}</version>
</dependency>
```

and add the following to your imports:
 
```
import ai.grakn.client.TaskClient;
```

The `BatchMutatorClient` makes use of the `TaskClient`. If your intention is to load data, please see our [mutator client documentation](../loader-api.html). 

# Basic Usage

The task client provides only one constructor that accepts the host and port where a single engine is running.

```java
TaskClient loader = TaskClient.of(String host, String port);
```

## Creating a task

The creating of tasks requires a number of parameters:

**Task Class**: Type of task to execute. This task class must be available to all of the servers on which it is to run. 

**Creator** String representing the class creating the task

**Execution time** Instant at which the task should be run. This can be at any time: if the time is before the current moment, the task will be run as soon as it reaches the Grakn engine executors. 

**Interval** Duration representing how often to run the task. If null, the task will only be executed once. 

**Configuration** Data on which to execute the task represented in a Json object

The `TaskClient.sendTask()` function accepts all of the above as arguments:

```java
TaskClient client = TaskClient.of(String host, String port);

Class taskClass = ShortExecutionMockTask.class;
String creator = this.getClass().getName();
Instant runAt = Instant.now();
Duration interval = Duration.ofSeconds(1);
Json configuration = Json.nil();

TaskId identifier = client.sendTask(taskClass, creator, runAt, interval, configuration);
```

This function will execute a `POST` request against the server that will then run the specified task asynchronously. The client is responsible for checking on the status of the task (see below).

## Stopping a task

To stop a task, its ID must be known. This method returns a boolean indicating if the task was sucessfully stopped by the server. 

```java
TaskClient client = TaskClient.of(host, port);

boolean stopped = client.stopTask(TaskId.of("randomTaskId");
```

## Status of a task

To check on the status of the task, its ID must be known. This method will throw an exception if the task cannot be found on the server. 

```java
TaskClient client = TaskClient.of(host, port);

TaskStatus status = client.getStatus(TaskId.of("randomTaskId"));
```

## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/23" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of the page.


{% include links.html %}

