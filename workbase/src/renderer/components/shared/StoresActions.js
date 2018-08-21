

// Schema Design store actions
export const DEFINE_ENTITY_TYPE = 'define-entity-type';
export const DEFINE_ATTRIBUTE_TYPE = 'define-attribute-type';
export const DEFINE_RELATIONSHIP_TYPE = 'define-relationship-type';
export const DEFINE_ROLE = 'define-role';
export const DELETE_TYPE = 'delete-type';
export const DELETE_SCHEMA_CONCEPT = 'delete-schema-concept';
export const LOAD_SCHEMA = 'load-schema';
export const ADD_TYPE = 'add-type';


// Data Management store actions
export const RUN_CURRENT_QUERY = 'run-current-query';
export const TOGGLE_LABEL = 'toggle-label';
export const TOGGLE_COLOUR = 'toggle-colour';


// Common actions shared by the two canvas stores (SchemaDesign && DataManagement)
export const LOAD_METATYPE_INSTANCES = 'load-metatype-instances';
export const INITIALISE_VISUALISER = 'initialise-visualiser';
export const CANVAS_RESET = 'canvas-reset';
export const CURRENT_KEYSPACE_CHANGED = 'current-keyspace-changed';

