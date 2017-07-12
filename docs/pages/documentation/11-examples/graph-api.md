---
title: Graph API Example
keywords: examples
last_updated: May 2017
tags: [getting-started, examples, java]
summary: "Learn how to use the Graph API to model an ontology in Java"
sidebar: documentation_sidebar
permalink: /documentation/examples/graph-api-example.html
folder: documentation
comment_issue_id: 27
---

This example shows how to use Java in a basic example that can be extended as a template for your own projects. It shows how to get set up, then how to build up an ontology, add data and how to make some queries. The example we will build is very simple: it's based on the genealogy dataset we have used throughout the GRAKN.AI documentation. We have kept it very simple (as close to a Hello World as you can get while still being useful as a template for creating and querying a graph). You can find it in our sample-projects repository on [Github](https://github.com/graknlabs/sample-projects/tree/master/example-graph-api-genealogy).

## Dependencies
All Grakn applications have the following Maven dependency:

```java
<dependency>
<groupId>ai.grakn</groupId>
<artifactId>grakn-graph</artifactId>
<version>${project.version}</version>
</dependency>
```

This dependency will give you access to the Core API. Your Java application will also require the following dependency when it is running against a Titan backend, which is what is configured for you by default:

```java
<dependency>
<groupId>ai.grakn</groupId>
<artifactId>titan-factory</artifactId>
<version>${project.version}</version>
</dependency>
```

If you want your server to run against a OrientDB backend instead, substitute `orientdb-factory` within `<artifactId></artifactId>` instead of `titan-factory`. Please note that, at this time, OrientDB support is still in early stages of development.

### Grakn Engine

First, make sure that you have an instance of Grakn engine running, which means that you need to run the following in the terminal:

```bash
cd [your Grakn install directory]
./bin/grakn.sh start
```


## Graph API: GraknGraph

The Graph API, `GraknGraph`, is a low-level API that encapsulates the [Grakn knowledge model](../the-fundamentals/grakn-knowledge-model.html). It provides Java object constructs for the Grakn ontological elements (entity types, relation types, etc.) and data instances (entities, relations, etc.), allowing you to build up a graph programmatically. It is also possible to perform simple concept lookups using the graph API, which I’ll illustrate presently. First, let’s look at building up the graph.

### Building the Ontology

We will look at the same ontology as is covered in the [Basic Ontology documentation](../building-an-ontology/basic-ontology.html) using Graql, which you may already be familiar with. If you’re not, the ontology is fully specified in Graql [here](../building-an-ontology/basic-ontology.html#the-complete-ontology). 

First we need a [graph](../developing-with-java/java-setup.html#initialising-a-transaction-on-the-graph):

```java
GraknSession session = Grakn.session(Grakn.DEFAULT_URI, keyspace);
GraknGraph graph = session.open(GraknTxType.WRITE)
```


Building the ontology is covered in `writeOntology()`. First, the method adds the resource types using putResourceType():

```java
identifier = graph.putResourceType("identifier", ResourceType.DataType.STRING);
name = graph.putResourceType("name", ResourceType.DataType.STRING);
firstname = graph.putResourceType("firstname", ResourceType.DataType.STRING).superType(name);
surname = graph.putResourceType("surname", ResourceType.DataType.STRING).superType(name);
middlename = graph.putResourceType("middlename", ResourceType.DataType.STRING).superType(name);
date = graph.putResourceType("date", ResourceType.DataType.DATE);
birthDate = graph.putResourceType("birth-date", ResourceType.DataType.DATE).superType(date);
deathDate = graph.putResourceType("death-date", ResourceType.DataType.DATE).superType(date);
gender = graph.putResourceType("gender", ResourceType.DataType.STRING);
```

Then it adds roles using `putRoleType()`:

```java
spouse = graph.putRoleType("spouse").setAbstract(true);
spouse1 = graph.putRoleType("spouse1").superType(spouse);
spouse2 = graph.putRoleType("spouse2").superType(spouse);
parent = graph.putRoleType("parent");
child = graph.putRoleType("child");
```

Then to add the relation types, `putRelationType()`, which is followed by `relates()` to set the roles associated with the relation and resource() to state that it has a date resource:

```java
marriage = graph.putRelationType("marriage");
marriage.relates(spouse1).relates(spouse2);
marriage.resource(date);
parentship = graph.putRelationType("parentship");
parentship.relates(parent).relates(child);
```

Finally, entity types are added using `putEntityType()`, `plays()` and `resource()`:

```java
person = graph.putEntityType("person");
person.plays(spouse1).plays(spouse2).plays(parent).plays(child);
person.resource(gender);
person.resource(birthDate);
person.resource(deathDate);
person.resource(identifier);
person.resource(firstname);
person.resource(middlename);
person.resource(surname);
```

Now to commit the ontology:

```java
graph.commit();
```

### Loading Data
Now that we have created the ontology, we can load in some data using the Graph API. 

The example project does this in `writeSampleRelation_Marriage()`. First it creates a person entity named homer:

```java
// Define the resources
Resource<String> firstNameJohn = firstname.putResource(“John”);
Resource<String> surnameNiesz = surname.putResource(“Niesz”);
Resource<String> male = gender.putResource(“Male”);
//Now we can create the actual husband entity
Entity johnNiesz = person.addEntity();
//Add the resources
johnNiesz.resource(firstNameJohn);
johnNiesz.resource(surnameNiesz);
johnNiesz.resource(male);
```

We can compare how a Graql statement maps to the Graph API. This is the equivalent in Graql:

```graql
insert $x isa person has firstname “John”, has surname “Niesz” has gender “male”;
```

The code goes on to create another `person` entity, named `maryYoung`, and then marries them:

```java
Relation theMarriage = marriage.addRelation().addRolePlayer(spouse1, johnNiesz).addRolePlayer(spouse2, maryYoung);
Resource marriageDate = date.putResource(LocalDateTime.of(1880, 8, 12, 0, 0, 0));
theMarriage.resource(marriageDate);
```

## Querying the Graph Using GraknGraph

The `runSampleQueries()` method shows how to run a simple query using the `GraknGraph` API. For example, take the query "What are the instances of type person?". In Graql, this is simply:

```graql
match $x isa person;
```

In Java:

```java
graph.getEntityType(“person”).instances().forEach(p->System.out.println(“ “ + p));
```

## Querying the Graph Using QueryBuilder

It is also possible to interact with the graph using a separate Java API that forms Graql queries. This is via `GraknGraph.graql()`, which returns a `QueryBuilder` object, discussed in the documentation. It is useful to use `QueryBuilder` if you want to make queries using Java, without having to construct a string containing the appropriate Graql expression. Taking the same query "What are the instances of type person?":

```java
graph.graql().match(var(“x”).isa(“person”)).execute().stream().
  map(Map::entrySet).forEach(p->System.out.println(“ “ + p));
``` 

Which leads us to the common question...

## When to use GraknGraph and when to use QueryBuilder?

**Graph API**
If you are primarily interested in mutating the graph, as well as doing simple concept lookups the Graph API will be sufficient, e.g. for
Manipulation, such as insertions into the graph.


**QueryBuilder — the “Java Graql” API**
This is best for advanced querying where traversals are involved. For example “Who is married to Homer?” is too complex a query for the Graph API. Using a `QueryBuilder`:

```java
List<Map<String, Concept>> results = graph.graql().match(
  var(“x”).has(“firstname”, “John”).isa(“person”),
  var(“y”).has(“firstname”, var(“y_name”)).isa(“person”),
  var().isa(“marriage”).
  rel(“husband”, “x”).
  rel(“wife”, “y”)).execute();
for (Map<String, Concept> result : results) {
  System.out.println(“ “ + result.get(“y_name”));
```


This example has been created, as much as anything, as a template that you can take to form the basis of your own projects. Feel free to add some more people to the graph, or make some additional queries. If you need some ideas, you’ll find extra examples of using Java Graql in the Graql documentation for match, insert, delete and aggregate queries.

## Where Next?
If you haven't already, please take a look at our [documentation on the Java APIs](../developing-with-java/java-setup.html), and our growing set of [Javadocs](https://grakn.ai/javadocs.html).
