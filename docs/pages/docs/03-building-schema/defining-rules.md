---
title: Defining Rules
keywords: graql, reasoner
tags: [graql, reasoning]
summary: "Graql Rules"
sidebar: documentation_sidebar
permalink: /docs/building-schema/defining-rules
folder: docs
---

## Introduction

Graql uses machine reasoning to perform inference over data and relationship types as well as to provide context disambiguation and dynamically-created relationships. This allows you to discover hidden and implicit associations between data instances through short and concise statements.

The rule-based reasoning allows automated capture and evolution of patterns within the knowledge graph. Graql reasoning is performed at query time and is guaranteed to be complete. Thanks to the reasoning facility, common patterns in the knowledge graph can be defined and associated with existing schema elements. The association happens by means of rules. This not only allows you to compress and simplify typical queries, but offers the ability to derive new non-trivial information by combining defined patterns.

Once a given query is executed, Graql will not only query the knowledge graph for exact matches but will also inspect the defined rules to check whether additional information can be found (inferred) by combining the patterns defined in the rules. The completeness property of Graql reasoning guarantees that, for a given content of the knowledge graph and the defined rule set, the query result shall contain all possible answers derived by combining database lookups and rule applications.

In this section we shall briefly describe the logics behind the rules as well as how can we define pattern associations by suitably defined rules. You may also want to review our [example of how to work with Graql rules](../examples/graql-reasoning).

## Graql Rules

Graql rules assume the following general form:

```
when {rule-body} then {rule-head}
```

People familiar with Prolog/Datalog, may recognise it as similar:

```
{rule-head} :- {rule-body}.
```

In logical terms, we restrict the rules to be definite Horn clauses. These can be defined either in terms of a disjunction with at most one unnegated atom or an implication with the consequent consisting of a single atom. Atoms are considered atomic first-order predicates - ones that cannot be decomposed to simpler constructs.

In our system we define both the head and the body of rules as Graql patterns. Consequently, the rules are statements of the form:

```
q1 ∧ q2 ∧ ... ∧ qn → p
```

where qs and the p are atoms that each correspond to a single Graql statement. The "when" of the statement (antecedent) then corresponds to the rule body with the "then" (consequent) corresponding to the rule head.

The implication form of Horn clauses aligns more naturally with Graql semantics as we define the rules in terms of the "when" and "then" blocks which directly correspond to the antecedent and consequent of the implication respectively.

## Graql Rule Syntax
In Graql we refer to the body of the rule as the "when" of the rule (antecedent of the implication) and the head as the "then" of the rule (consequent of the implication). Therefore, in Graql terms, we define rule objects in the following way:

```graql-test-ignore
optional-label
when {
    ...;
    ...;
    ...;
},
then {
    ...;
};
```

Each dotted line corresponds to a single Graql variable pattern. The rule label is optional and can be omitted, but it is useful if we want to be able to refer to and identify particular rules in the knowledge graph.

In Graql, the "when" of the rule is required to be a [conjunctive pattern](https://en.wikipedia.org/wiki/Logical_conjunction), whereas the "then" should be atomic - contain a single pattern. If your use case requires a rule with a disjunction in the "when" part, please notice that, when using the disjunctive normal form, it can be decomposed into series of conjunctive rules.

### Defining rules
Rules are treated as inherent members of the schema. As a result they require the `define` keyword for their creation. A rule is then defined by specifying its label followed
by `when` and `then` blocks. The label is necessary if we want to refer to the rule later.

A classic reasoning example is the ancestor example. The two Graql rules `R1` and `R2` stated below define the ancestor relationship, which can be understood as either happening between two generations directly between a parent and a child or between three generations when the first generation hop is expressed via a parentship relationship and the second generation hop is captured by an ancestor relationship.

```graql
define

R1
when {
    (parent: $p, child: $c) isa Parent;
},
then {
    (ancestor: $p, descendant: $c) isa Ancestor;
};

R2
when {
    (parent: $p, child: $c) isa Parent;
    (ancestor: $c, descendant: $d) isa Ancestor;
},
then {
    (ancestor: $p, descendant: $d) isa Ancestor;
};
```


Defining the above rules in terms of predicates and assuming left-to-right directionality of the roles, we can summarise them in the implication form as:

```
R1: parent(X, Y) → ancestor(X, Y)  
R2: parent(X, Z) ∧ ancestor(Z, Y) → ancestor(X, Y)
```

### Retrieving defined rules

To retrieve rules, we refer to them by their label in a `match` statement:

```
match $x label R1; get;
match $x label R2; get;
```

or as a joint statement:

```
match {$x label R1;} or {$x label R2;}; get;
```

### Deleting rules

To delete rules we refer to them by their label and use the `undefine` keyword. For the case of the rules defined above, to delete them we write:

```
undefine R1 sub rule;
undefine R2 sub rule;
```

To persist the result, executing a `commit;` statement is also required.

## Allowed Graql Constructs in Rules
The tables below summarise Graql constructs that are allowed to appear in a when-then (body-head) blocks of a rule.

We define atomic queries as queries that contain at most one potentially rule-resolvable statement.
That means atomic queries contain at most one statement that can potentially appear in the "then" of any rule.

### Queries

| Description        | when | then
| -------------------- |:--|:--|
| atomic queries | ✓ | ✓ |
| conjunctive queries        | ✓ | x |
| disjunctive queries        | x | x |  

### Variable Patterns

Below are listed the variable patterns that are allowed to be defined in rule heads together with specific remarks.

| Description        | Pattern Example           | remarks
| -------------------- |:--- |:------|
| `entity` | `$x isa person;` | type strictly needs to be specified, variable needs to be bound |
| `attribute` | `$x has age 20;` | value predicate needs to be specific (=) |
| `relationship` | `(parent: $x, child: $y) isa parentship;` | type strictly needs to be specified, all roles need to be explicitly specified and non-meta |

### Type Properties

Type properties deal with schema elements and are not allowed to form rule heads.

| Description        | Pattern Example   | when | then
| -------------------- |:---|:--|:--|
| `sub`        | `$x sub type;` | ✓| x |
| `plays` | `$x plays parent;` |✓| x |
| `has`        | `$x has firstname;` | ✓ | x |  
| `relates`   | `marriage relates $x;` | ✓ | x |
| `is-abstract` | `$x is-abstract;` | ✓ | x |
| `datatype` | `$x isa attribute, datatype string;` | ✓| x |
| `regex` | `$x isa attribute, regex /hello/;` | ✓ | x |

## Where Next?

There is a complete [example of how to work with Graql rules](../examples/graql-reasoning) available, and reasoning is also discussed in our [quick start tutorial](../get-started/quickstart-tutorial).
