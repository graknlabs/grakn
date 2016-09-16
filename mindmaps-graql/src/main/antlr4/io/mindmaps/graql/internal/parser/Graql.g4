grammar Graql;

queryEOF       : query EOF ;
query          : matchQuery | askQuery | insertQuery | deleteQuery | aggregateQuery | computeQuery ;

matchEOF       : matchQuery EOF ;
askEOF         : askQuery EOF ;
insertEOF      : insertQuery EOF ;
deleteEOF      : deleteQuery EOF ;
aggregateEOF   : aggregateQuery EOF ;
computeEOF     : computeQuery EOF ;

matchQuery     : 'match' patterns modifiers ;
askQuery       : matchQuery 'ask' ;
insertQuery    : matchQuery? 'insert' varPatterns ;
deleteQuery    : matchQuery 'delete' varPatterns ;
aggregateQuery : matchQuery 'aggregate' aggregate ;
computeQuery   : 'compute' id ('in' subgraph)? ;

subgraph       : id (',' id) * ;

aggregate      : id argument*                     # customAgg
               | '(' namedAgg (',' namedAgg)* ')' # selectAgg
               ;
argument       : VARIABLE  # variableArgument
               | aggregate # aggregateArgument
               ;
namedAgg       : aggregate 'as' id ;

patterns       : pattern (';' pattern)* ';'? ;
pattern        : varPattern                    # varPatternCase
               | pattern 'or' pattern          # orPattern
               | '{' patterns '}'              # andPattern
               ;

varPatterns    : varPattern (';' varPattern)* ';'? ;
varPattern     : variable | variable? property (','? property)* ;

property       : 'isa' variable                   # isa
               | 'ako' variable                   # ako
               | 'has-role' variable              # hasRole
               | 'plays-role' variable            # playsRole
               | 'has-scope' variable             # hasScope
               | 'id' STRING                      # propId
               | 'value' predicate?               # propValue
               | 'lhs' '{' query '}'              # propLhs
               | 'rhs' '{' query '}'              # propRhs
               | 'has' id (predicate | VARIABLE)? # propHas
               | 'has-resource' variable          # propResource
               | '(' casting (',' casting)* ')'   # propRel
               | 'is-abstract'                    # isAbstract
               | 'datatype' DATATYPE              # propDatatype
               | 'regex' REGEX                    # propRegex
               ;

casting        : (variable ':')? variable ;

variable       : id | VARIABLE ;

predicate      : '='? value                # predicateEq
               | '!=' value                # predicateNeq
               | '>' value                 # predicateGt
               | '>=' value                # predicateGte
               | '<' value                 # predicateLt
               | '<=' value                # predicateLte
               | 'contains' STRING         # predicateContains
               | REGEX                     # predicateRegex
               | predicate 'and' predicate # predicateAnd
               | predicate 'or' predicate  # predicateOr
               | '(' predicate ')'         # predicateParens
               ;
value          : STRING  # valueString
               | INTEGER # valueInteger
               | REAL    # valueReal
               | BOOLEAN # valueBoolean
               ;

modifiers      : (','? modifier)* ;
modifier       : 'select' VARIABLE (',' VARIABLE)* # modifierSelect
               | 'limit' INTEGER                   # modifierLimit
               | 'offset' INTEGER                  # modifierOffset
               | 'distinct'                        # modifierDistinct
               | 'order' 'by' VARIABLE ORDER?      # modifierOrderBy
               ;

// This rule is used for parsing streams of patterns separated by semicolons
patternSep     : pattern ';' ;

id             : ID | STRING ;

DATATYPE       : 'long' | 'double' | 'string' | 'boolean' ;
ORDER          : 'asc' | 'desc' ;
BOOLEAN        : 'true' | 'false' ;
VARIABLE       : '$' [a-zA-Z0-9_-]+ ;
ID             : [a-zA-Z_] [a-zA-Z0-9_-]* ;
STRING         : '"' (~["\\] | ESCAPE_SEQ)* '"' | '\'' (~['\\] | ESCAPE_SEQ)* '\'';
REGEX          : '/' (~'/' | '\\/')* '/' ;
INTEGER        : ('+' | '-')? [0-9]+ ;
REAL           : ('+' | '-')? [0-9]+ '.' [0-9]+ ;

fragment ESCAPE_SEQ : '\\' . ;

COMMENT : '#' .*? '\r'? ('\n' | EOF) -> skip ;

WS : [ \t\r\n]+ -> skip ;

// Unused lexer rule to help with autocomplete on variable names
DOLLAR : '$' ;