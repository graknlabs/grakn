---
title: Java API Example
keywords: examples
tags: [getting-started, examples, java]
summary: "Learn how to use the Java API to model a schema"
sidebar: documentation_sidebar
permalink: /docs/examples/java-api-example
folder: docs
---

This example shows how to use Java in a basic example that can be extended as a template for your own projects. It shows how to get set up, then how to build up a schema, add data and how to make some queries. The example we will build is very simple: it's based on the genealogy dataset we have used throughout the GRAKN.AI documentation. We have kept it very simple (as close to a Hello World as you can get while still being useful as a template for creating and querying a knowledge graph). You can find it in our sample-projects repository on [Github](https://github.com/graknlabs/sample-projects/tree/master/example-java-api-genealogy).

## Dependencies
All Grakn applications have the following Maven dependency:

```xml
<repositories>
  <repository>
    <id>releases</id>
    <url>https://oss.sonatype.org/content/repositories/releases</url>
  </repository>
</repositories>

<properties>
    <grakn.version>1.3.0</grakn.version>
</properties>

<dependencies>
  <dependency>
    <groupId>ai.grakn</groupId>
    <artifactId>client-java</artifactId>
    <version>${grakn.version}</version>
  </dependency>
</dependencies>
```

### Grakn Engine

First, make sure that you have an instance of Grakn engine running, which means that you need to run the following in the terminal:

```bash
cd [your Grakn install directory]
./grakn server start
```


## Java API: Grakn.Transaction

The Java API, `Grakn.Transaction`, is a low-level API that encapsulates the [Grakn knowledge model](../knowledge-model/model). It provides Java object constructs for the Grakn ontological elements (entity types, relationship types, etc.) and data instances (entities, relationships, etc.), allowing you to build up a knowledge graph programmatically. It is also possible to perform simple concept lookups using the java API, which I’ll illustrate presently. First, let’s look at building up the knowledge graph.

### Building the Schema

We will look at the same schema as is covered in the [Basic Schema documentation](../building-schema/basic-schema) using Graql, which you may already be familiar with. If you’re not, the schema is fully specified in Graql [here](../building-schema/basic-schema#the-complete-schema).

First we need a [knowledge graph](../java-library/setup#initialising-a-transaction-on-the-knowledge-base):

```java-test-ignore
Grakn grakn = new Grakn(new SimpleURI("localhost:48555"));
Grakn.Session session = grakn.session(keyspace);
Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)
```


Building the schema is covered in `writeSchema()`. First, the method adds the attribute types using putAttributeType():

```java-test-ignore
identifier = tx.putAttributeType("identifier", AttributeType.DataType.STRING);
name = tx.putAttributeType("name", AttributeType.DataType.STRING);
firstname = tx.putAttributeType("firstname", AttributeType.DataType.STRING).sup(name);
surname = tx.putAttributeType("surname", AttributeType.DataType.STRING).sup(name);
middlename = tx.putAttributeType("middlename", AttributeType.DataType.STRING).sup(name);
eventDate = tx.putAttributeType("event-date", AttributeType.DataType.DATE);
birthDate = tx.putAttributeType("birth-date", AttributeType.DataType.DATE).sup(eventDate);
deathDate = tx.putAttributeType("death-date", AttributeType.DataType.DATE).sup(eventDate);
gender = tx.putAttributeType("gender", AttributeType.DataType.STRING);
```

Then it adds roles using `putRole()`:

```java-test-ignore
spouse = tx.putRole("spouse");
spouse1 = tx.putRole("spouse1").sup(spouse);
spouse2 = tx.putRole("spouse2").sup(spouse);
parent = tx.putRole("parent");
child = tx.putRole("child");
```

Then to add the relationship types, `putRelationshipType()`, which is followed by `relates()` to set the roles associated with the relationship and attribute() to state that it has a date attribute:

```java-test-ignore
marriage = tx.putRelationshipType("marriage");
marriage.relates(spouse).relates(spouse1).relates(spouse2);
marriage.has(eventDate);
parentship = tx.putRelationshipType("parentship");
parentship.relates(parent).relates(child);
```

Finally, entity types are added using `putEntityType()`, `plays()` and `attribute()`:

```java-test-ignore
person = tx.putEntityType("person");
person.plays(spouse1).plays(spouse2).plays(parent).plays(child);
person.has(gender);
person.has(birthDate);
person.has(deathDate);
person.has(identifier);
person.has(firstname);
person.has(middlename);
person.has(surname);
```

Now to commit the schema:

```java-test-ignore
tx.commit();
```

### Loading Data
Now that we have created the schema, we can load in some data using the Java API.

The example project does this in `writeSampleRelation_Marriage()`. First it creates a person entity named homer:

```java-test-ignore
// After committing we need to open a new transaction
tx = session.transaction(GraknTxType.WRITE)

// Define the attributes
Attribute<String> firstNameJohn = firstname.create("John");
Attribute<String> surnameNiesz = surname.create("Niesz");
Attribute<String> male = gender.create("male");
//Now we can create the actual husband entity
Entity johnNiesz = person.create();
//Add the attributes
johnNiesz.has(firstNameJohn);
johnNiesz.has(surnameNiesz);
johnNiesz.has(male);
```

We can compare how a Graql statement maps to the Java API. This is the equivalent in Graql:

```graql
insert $x isa person has firstname "John", has surname "Niesz" has gender "male";
```

The code goes on to create another `person` entity, named `maryYoung`, and then marries them:

```java-test-ignore
Entity maryYoung = person.create();

Relationship theMarriage = marriage.create().assign(spouse1, johnNiesz).assign(spouse2, maryYoung);
Attribute marriageDate = eventDate.create(LocalDateTime.of(1880, 8, 12, 0, 0, 0));
theMarriage.has(marriageDate);
```

## Querying the Knowledge Graph Using GraknTx

The `runSampleQueries()` method shows how to run a simple query using the `GraknTx` API. For example, take the query "What are the instances of type person?". In Graql, this is simply:

```graql
match $x isa person; get;
```

In Java:

```java-test-ignore
for (Thing p: tx.getEntityType("person").instances()) {
    System.out.println(" " + p);
}
```

## Querying the Knowledge Graph Using QueryBuilder

It is also possible to interact with the knowledge graph using a separate Java API that forms Graql queries. This is via `Grakn.Transaction.graql()`, which returns a `QueryBuilder` object, discussed in the documentation. It is useful to use `QueryBuilder` if you want to make queries using Java, without having to construct a string containing the appropriate Graql expression. Taking the same query "What are the instances of type person?":

```java-test-ignore
for (ConceptMap a: tx.graql().match(var("x").isa("person")).get().execute()) {
    System.out.println(" " + a);
}
```

Which leads us to the common question...

## When to use Grakn.Transaction and when to use QueryBuilder?

**Java API**
If you are primarily interested in mutating the knowledge graph, as well as doing simple concept lookups the Java API will be sufficient, e.g. for
Manipulation, such as insertions into the knowledge graph.


**QueryBuilder — the “Java Graql” API**
This is best for advanced querying where traversals are involved. For example “Who is married to Homer?” is too complex a query for the Java API. Using a `QueryBuilder`:

```java-test-ignore
GetQuery query = tx.graql().match(
  var("x").has("firstname", "John").isa("person"),
  var("y").has("firstname", var("y_name")).isa("person"),
  var().isa("marriage").
  rel("husband", "x").
  rel("wife", "y")).get();
for (Map<String, Concept> result : query) {
  System.out.println(" " + result.get("y_name"));
}

tx.close();
```


This example has been created, as much as anything, as a template that you can take to form the basis of your own projects. Feel free to add some more people to the knowledge graph, or make some additional queries. If you need some ideas, you’ll find extra examples of using Java Graql in the Graql documentation for match, insert, delete and aggregate queries.

## Where Next?
If you haven't already, please take a look at our [documentation on the Java APIs](../java-library/setup), and our growing set of [Javadocs](http://javadoc.io/doc/ai.grakn/grakn).
