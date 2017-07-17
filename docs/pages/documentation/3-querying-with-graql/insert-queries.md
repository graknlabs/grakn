---
title: Insert Queries
keywords: graql, query, insert
last_updated: August 10, 2016
tags: [graql]
summary: "Graql Insert Queries"
sidebar: documentation_sidebar
permalink: /documentation/graql/insert-queries.html
folder: documentation
---

The page documents use of the Graql `insert` query, which will insert a specified [variable pattern](#variable-patterns) into the graph. To follow along, or experiment further, with the examples given below, please load the *basic-genealogy.gql* file, which can be found in the *examples* directory of the Grakn installation zip, or on [Github](https://github.com/graknlabs/grakn/blob/master/grakn-dist/src/examples/basic-genealogy.gql).

{% include note.html content="If you are working in the Graql shell, don't forget to `commit` to store an insertion in the graph." %}


## `match-insert`

If a [match query](match-queries.html) is provided, the query will insert the given variable patterns for every result of the query.
The pattern describes [properties](#properties) to set on a particular concept and can optionally be bound to a variable or an ID.

In the example below, we insert additional (fictional) information for a `person` entity who we have matched through `identifier` Mary Guthrie.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell1" data-toggle="tab">Graql</a></li>
    <li><a href="#java1" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell1">
<pre>
match $p has identifier "Mary Guthrie"; insert $p has middlename "Mathilda"; $p has birth-date "1902-01-01"; $p has death-date "1952-01-01"; $p has age 50;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java1">
<pre>
qb.match(var("p").has("identifier", "Mary Guthrie"))
    .insert(var("p").has("middlename", "Mathilda"), 
        var("p").has("birth-date", LocalDateTime.of(1902, 1, 1, 0, 0, 0).toString()),
        var("p").has("death-date", LocalDateTime.of(1952, 1, 1, 0, 0, 0).toString()),
        var("p").has("age", 50)
    ).execute();
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->


## Properties

### isa

Set the type of the inserted concept.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell2" data-toggle="tab">Graql</a></li>
    <li><a href="#java2" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell2">
<pre>
insert has identifier "Titus Groan" isa person;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java2">
<pre>
qb.insert(var().has("identifier", "Titus Groan").isa("person")).execute();
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->


### id

It is not possible to insert a concept with the given id, as this is the job of the system. However, if you attempt to insert by id, you will retrieve a concept if one with that id already exists. The created or retrieved concept can then be modified with further properties.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell3" data-toggle="tab">Graql</a></li>
    <li><a href="#java3" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell3">
<pre>
insert id "1376496" isa person;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java3">
<pre>
qb.insert(var().id(ConceptId.of("1376496")).isa("person")).execute();
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->


### val

Set the value of the concept.
<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell4" data-toggle="tab">Graql</a></li>
    <li><a href="#java4" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell4">
<pre>
insert val "Ash" isa surname;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java4">
<pre>
qb.insert(var().val("Ash").isa("surname")).execute();
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

### has

Add a resource of the given type to the concept.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell5" data-toggle="tab">Graql</a></li>
    <li><a href="#java5" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell5">
<pre>
insert isa person, has identifier "Fuchsia Groan" has gender "female";
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java5">
<pre>
qb.insert(var().isa("person").has("identifier", "Fuchsia Groan").has("gender", "female")).execute();
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

### relation

Make the concept a relation that relates the given role players, playing the given roles.   
*(With apologies to 'Gormenghast' fans, who will be aware that Titus and Fuchsia are siblings and thus cannot marry).*

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell7" data-toggle="tab">Graql</a></li>
    <li><a href="#java7" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell7">
<pre>
match $p1 has identifier "Titus Groan"; $p2 has identifier "Fuchsia Groan"; insert (spouse1: $p1, spouse2: $p2) isa marriage;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java7">
<pre>
qb.match(
  var("p1").has("name", "Titus Groan"),
  var("p2").has("name", "Fuchsia Groan")
).insert(
  var()
    .rel("spouse1", "p1")
    .rel("spouse2", "p2")
    .isa("marriage")
).execute();

</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->


## Type Properties

The following properties only apply to types.

### sub

Set up a hierarchy.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell8" data-toggle="tab">Graql</a></li>
    <li><a href="#java8" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell8">
<pre>
insert man sub person;
insert woman sub person;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java8">
<pre>
qb.insert(label("man").sub("person")).execute();
qb.insert(label("woman").sub("person")).execute();
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->


### relates
Add a role to a relation.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell9" data-toggle="tab">Graql</a></li>
    <li><a href="#java9" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell9">
<pre>
insert siblings sub relation, relates sibling1, relates sibling2;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java9">
<pre>
qb.insert(
  label("siblings").sub("relation")
    .relates("sibling1").relates("sibling2")
).execute();
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->


### plays
Allow the concept type to play the given role.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell10" data-toggle="tab">Graql</a></li>
    <li><a href="#java10" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell10">
<pre>
insert person plays sibling1;
insert person plays sibling2;
</pre>
</div>
<div role="tabpanel" class="tab-pane" id="java10">
<pre>
qb.insert(label("person").plays("sibling1")).execute();
qb.insert(label("person").plays("sibling2")).execute();
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->


### has

Allow the concept type to have the given resource.

This is done by creating a specific relation relating the concept and resource.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a href="#shell11" data-toggle="tab">Graql</a></li>
    <li><a href="#java11" data-toggle="tab">Java</a></li>
</ul>

<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="shell11">
<pre>
insert person has nickname;
</pre>
</div>

<div role="tabpanel" class="tab-pane" id="java11">
<pre>
qb.insert(label("person").has("nickname")).execute();
</pre>
</div> <!-- tab-pane -->
</div> <!-- tab-content -->

## Comments
Want to leave a comment? Visit <a href="https://github.com/graknlabs/docs/issues/42" target="_blank">the issues on Github for this page</a> (you'll need a GitHub account). You are also welcome to contribute to our documentation directly via the "Edit me" button at the top of the page.


{% include links.html %}
has