use super::sqlstate::SqlState;

pub const SUCCESSFUL_COMPLETION: SqlState = SqlState::new(*b"00000");

pub const WARNING: SqlState = SqlState::new(*b"01000");

pub const NULL_VALUE_ELIMINATED_IN_SET_FUNCTION: SqlState = SqlState::new(*b"01003");

pub const STRING_DATA_RIGHT_TRUNCATION_WARNING: SqlState = SqlState::new(*b"01004");

pub const PRIVILEGE_NOT_REVOKED: SqlState = SqlState::new(*b"01006");

pub const PRIVILEGE_NOT_GRANTED: SqlState = SqlState::new(*b"01007");

pub const NO_DATA: SqlState = SqlState::new(*b"02000");

pub const NO_ADDITIONAL_DYNAMIC_RESULT_SETS_RETURNED: SqlState = SqlState::new(*b"02001");

pub const DYNAMIC_SQL_ERROR: SqlState = SqlState::new(*b"07000");

pub const CONNECTION_EXCEPTION: SqlState = SqlState::new(*b"08000");

pub const CONNECTION_DOES_NOT_EXIST: SqlState = SqlState::new(*b"08003");

pub const CONNECTION_FAILURE: SqlState = SqlState::new(*b"08006");

pub const FEATURE_NOT_SUPPORTED: SqlState = SqlState::new(*b"0A000");

pub const CARDINALITY_VIOLATION: SqlState = SqlState::new(*b"21000");

pub const DATA_EXCEPTION: SqlState = SqlState::new(*b"22000");

pub const STRING_DATA_RIGHT_TRUNCATION: SqlState = SqlState::new(*b"22001");

pub const NULL_VALUE_NO_INDICATOR_PARAMETER: SqlState = SqlState::new(*b"22002");

pub const NUMERIC_VALUE_OUT_OF_RANGE: SqlState = SqlState::new(*b"22003");

pub const NULL_VALUE_NOT_ALLOWED: SqlState = SqlState::new(*b"22004");

pub const ERROR_IN_ASSIGNMENT: SqlState = SqlState::new(*b"22005");

pub const INVALID_DATETIME_FORMAT: SqlState = SqlState::new(*b"22007");

pub const DATETIME_FIELD_OVERFLOW: SqlState = SqlState::new(*b"22008");

pub const INVALID_TIME_ZONE_DISPLACEMENT_VALUE: SqlState = SqlState::new(*b"22009");

pub const ESCAPE_CHARACTER_CONFLICT: SqlState = SqlState::new(*b"2200B");

pub const INVALID_USE_OF_ESCAPE_CHARACTER: SqlState = SqlState::new(*b"2200C");

pub const INVALID_ESCAPE_OCTET: SqlState = SqlState::new(*b"2200D");

pub const ZERO_LENGTH_CHARACTER_STRING: SqlState = SqlState::new(*b"2200F");

pub const DIVISION_BY_ZERO: SqlState = SqlState::new(*b"22012");

pub const INTERVAL_FIELD_OVERFLOW: SqlState = SqlState::new(*b"22015");

pub const INVALID_CHARACTER_VALUE_FOR_CAST: SqlState = SqlState::new(*b"22018");

pub const INVALID_ESCAPE_CHARACTER: SqlState = SqlState::new(*b"22019");

pub const INVALID_REGULAR_EXPRESSION: SqlState = SqlState::new(*b"2201B");

pub const INVALID_ARGUMENT_FOR_LOG: SqlState = SqlState::new(*b"2201E");

pub const INVALID_ARGUMENT_FOR_POWER_FUNCTION: SqlState = SqlState::new(*b"2201F");

pub const INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION: SqlState = SqlState::new(*b"2201G");

pub const INVALID_ROW_COUNT_IN_LIMIT_CLAUSE: SqlState = SqlState::new(*b"2201W");

pub const INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE: SqlState = SqlState::new(*b"2201X");

pub const CHARACTER_NOT_IN_REPERTOIRE: SqlState = SqlState::new(*b"22021");

pub const INDICATOR_OVERFLOW: SqlState = SqlState::new(*b"22022");

pub const INVALID_PARAMETER_VALUE: SqlState = SqlState::new(*b"22023");

pub const UNTERMINATED_C_STRING: SqlState = SqlState::new(*b"22024");

pub const INVALID_ESCAPE_SEQUENCE: SqlState = SqlState::new(*b"22025");

pub const STRING_DATA_LENGTH_MISMATCH: SqlState = SqlState::new(*b"22026");

pub const TRIM_ERROR: SqlState = SqlState::new(*b"22027");

pub const ARRAY_SUBSCRIPT_ERROR: SqlState = SqlState::new(*b"2202E");

pub const INVALID_JSON_TEXT: SqlState = SqlState::new(*b"22032");

pub const INVALID_SQL_JSON_SUBSCRIPT: SqlState = SqlState::new(*b"22033");

pub const MORE_THAN_ONE_SQL_JSON_ITEM: SqlState = SqlState::new(*b"22034");

pub const NO_SQL_JSON_ITEM: SqlState = SqlState::new(*b"22035");

pub const NON_NUMERIC_SQL_JSON_ITEM: SqlState = SqlState::new(*b"22036");

pub const NON_UNIQUE_KEYS_IN_JSON_OBJECT: SqlState = SqlState::new(*b"22037");

pub const SINGLETON_SQL_JSON_ITEM_REQUIRED: SqlState = SqlState::new(*b"22038");

pub const SQL_JSON_ARRAY_NOT_FOUND: SqlState = SqlState::new(*b"22039");

pub const SQL_JSON_MEMBER_NOT_FOUND: SqlState = SqlState::new(*b"2203A");

pub const SQL_JSON_NUMBER_NOT_FOUND: SqlState = SqlState::new(*b"2203B");

pub const SQL_JSON_OBJECT_NOT_FOUND: SqlState = SqlState::new(*b"2203C");

pub const TOO_MANY_JSON_ARRAY_ELEMENTS: SqlState = SqlState::new(*b"2203D");

pub const TOO_MANY_JSON_OBJECT_MEMBERS: SqlState = SqlState::new(*b"2203E");

pub const SQL_JSON_SCALAR_REQUIRED: SqlState = SqlState::new(*b"2203F");

pub const INVALID_JSON_PATH: SqlState = SqlState::new(*b"22030");

pub const INVALID_CAST_SPECIFICATION: SqlState = SqlState::new(*b"22018");

pub const INTEGRITY_CONSTRAINT_VIOLATION: SqlState = SqlState::new(*b"23000");

pub const RESTRICT_VIOLATION: SqlState = SqlState::new(*b"23001");

pub const NOT_NULL_VIOLATION: SqlState = SqlState::new(*b"23502");

pub const FOREIGN_KEY_VIOLATION: SqlState = SqlState::new(*b"23503");

pub const UNIQUE_VIOLATION: SqlState = SqlState::new(*b"23505");

pub const CHECK_VIOLATION: SqlState = SqlState::new(*b"23514");

pub const EXCLUSION_VIOLATION: SqlState = SqlState::new(*b"23P01");

pub const INVALID_CURSOR_STATE: SqlState = SqlState::new(*b"24000");

pub const INVALID_TRANSACTION_STATE: SqlState = SqlState::new(*b"25000");

pub const ACTIVE_SQL_TRANSACTION: SqlState = SqlState::new(*b"25001");

pub const BRANCH_TRANSACTION_ALREADY_ACTIVE: SqlState = SqlState::new(*b"25002");

pub const INAPPROPRIATE_ACCESS_MODE_FOR_BRANCH_TRANSACTION: SqlState = SqlState::new(*b"25003");

pub const INAPPROPRIATE_ISOLATION_LEVEL_FOR_BRANCH_TRANSACTION: SqlState = SqlState::new(*b"25004");

pub const NO_ACTIVE_SQL_TRANSACTION_FOR_BRANCH_TRANSACTION: SqlState = SqlState::new(*b"25005");

pub const READ_ONLY_SQL_TRANSACTION: SqlState = SqlState::new(*b"25006");

pub const SCHEMA_AND_DATA_STATEMENT_MIXING_NOT_SUPPORTED: SqlState = SqlState::new(*b"25007");

pub const HELD_CURSOR_REQUIRES_SAME_ISOLATION_LEVEL: SqlState = SqlState::new(*b"25008");

pub const NO_ACTIVE_SQL_TRANSACTION: SqlState = SqlState::new(*b"25P01");

pub const IN_FAILED_SQL_TRANSACTION: SqlState = SqlState::new(*b"25P02");

pub const INVALID_SQL_STATEMENT_NAME: SqlState = SqlState::new(*b"26000");

pub const INVALID_AUTHORIZATION_SPECIFICATION: SqlState = SqlState::new(*b"28000");

pub const INVALID_PASSWORD: SqlState = SqlState::new(*b"28P01");

pub const DEPENDENT_PRIVILEGE_DESCRIPTORS_STILL_EXIST: SqlState = SqlState::new(*b"2B000");

pub const DEPENDENT_OBJECTS_STILL_EXIST: SqlState = SqlState::new(*b"2BP01");

pub const INVALID_TRANSACTION_TERMINATION: SqlState = SqlState::new(*b"2D000");

pub const SQL_ROUTINE_EXCEPTION: SqlState = SqlState::new(*b"2F000");

pub const FUNCTION_EXECUTED_NO_RETURN_STATEMENT: SqlState = SqlState::new(*b"2F005");

pub const MODIFYING_SQL_DATA_NOT_PERMITTED: SqlState = SqlState::new(*b"2F002");

pub const PROHIBITED_SQL_STATEMENT_ATTEMPTED: SqlState = SqlState::new(*b"2F003");

pub const READING_SQL_DATA_NOT_PERMITTED: SqlState = SqlState::new(*b"2F004");

pub const INVALID_CURSOR_NAME: SqlState = SqlState::new(*b"34000");

pub const EXTERNAL_ROUTINE_EXCEPTION: SqlState = SqlState::new(*b"38000");

pub const CONTAINING_SQL_NOT_PERMITTED: SqlState = SqlState::new(*b"38001");

pub const MODIFYING_SQL_DATA_NOT_PERMITTED_EXTERNAL: SqlState = SqlState::new(*b"38002");

pub const PROHIBITED_SQL_STATEMENT_ATTEMPTED_EXTERNAL: SqlState = SqlState::new(*b"38003");

pub const READING_SQL_DATA_NOT_PERMITTED_EXTERNAL: SqlState = SqlState::new(*b"38004");

pub const EXTERNAL_ROUTINE_INVOCATION_EXCEPTION: SqlState = SqlState::new(*b"39000");

pub const INVALID_SQLSTATE_RETURNED: SqlState = SqlState::new(*b"39001");

pub const NULL_VALUE_NOT_ALLOWED_EXTERNAL: SqlState = SqlState::new(*b"39004");

pub const SAVEPOINT_EXCEPTION: SqlState = SqlState::new(*b"3B000");

pub const INVALID_SAVEPOINT_SPECIFICATION: SqlState = SqlState::new(*b"3B001");

pub const INVALID_CATALOG_NAME: SqlState = SqlState::new(*b"3D000");

pub const INVALID_SCHEMA_NAME: SqlState = SqlState::new(*b"3F000");

pub const TRANSACTION_ROLLBACK: SqlState = SqlState::new(*b"40000");

pub const TRANSACTION_INTEGRITY_CONSTRAINT_VIOLATION: SqlState = SqlState::new(*b"40002");

pub const SERIALIZATION_FAILURE: SqlState = SqlState::new(*b"40001");

pub const STATEMENT_COMPLETION_UNKNOWN: SqlState = SqlState::new(*b"40003");

pub const DEADLOCK_DETECTED: SqlState = SqlState::new(*b"40P01");

pub const SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION: SqlState = SqlState::new(*b"42000");

pub const SYNTAX_ERROR: SqlState = SqlState::new(*b"42601");

pub const INSUFFICIENT_PRIVILEGE: SqlState = SqlState::new(*b"42501");

pub const INVALID_NAME: SqlState = SqlState::new(*b"42602");

pub const INVALID_COLUMN_DEFINITION: SqlState = SqlState::new(*b"42611");

pub const NAME_TOO_LONG: SqlState = SqlState::new(*b"42622");

pub const DUPLICATE_COLUMN: SqlState = SqlState::new(*b"42701");

pub const AMBIGUOUS_COLUMN: SqlState = SqlState::new(*b"42702");

pub const UNDEFINED_COLUMN: SqlState = SqlState::new(*b"42703");

pub const UNDEFINED_OBJECT: SqlState = SqlState::new(*b"42704");

pub const DUPLICATE_OBJECT: SqlState = SqlState::new(*b"42710");

pub const DUPLICATE_ALIAS: SqlState = SqlState::new(*b"42712");

pub const DUPLICATE_FUNCTION: SqlState = SqlState::new(*b"42723");

pub const AMBIGUOUS_FUNCTION: SqlState = SqlState::new(*b"42725");

pub const GROUPING_ERROR: SqlState = SqlState::new(*b"42803");

pub const DATATYPE_MISMATCH: SqlState = SqlState::new(*b"42804");

pub const WRONG_OBJECT_TYPE: SqlState = SqlState::new(*b"42809");

pub const INVALID_FOREIGN_KEY: SqlState = SqlState::new(*b"42830");

pub const CANNOT_COERCE: SqlState = SqlState::new(*b"42846");

pub const UNDEFINED_FUNCTION: SqlState = SqlState::new(*b"42883");

pub const RESERVED_NAME: SqlState = SqlState::new(*b"42939");

pub const UNDEFINED_TABLE: SqlState = SqlState::new(*b"42P01");

pub const UNDEFINED_PARAMETER: SqlState = SqlState::new(*b"42P02");

pub const DUPLICATE_CURSOR: SqlState = SqlState::new(*b"42P03");

pub const DUPLICATE_DATABASE: SqlState = SqlState::new(*b"42P04");

pub const DUPLICATE_PREPARED_STATEMENT: SqlState = SqlState::new(*b"42P05");

pub const DUPLICATE_SCHEMA: SqlState = SqlState::new(*b"42P06");

pub const DUPLICATE_TABLE: SqlState = SqlState::new(*b"42P07");

pub const AMBIGUOUS_PARAMETER: SqlState = SqlState::new(*b"42P08");

pub const AMBIGUOUS_ALIAS: SqlState = SqlState::new(*b"42P09");

pub const INVALID_COLUMN_REFERENCE: SqlState = SqlState::new(*b"42P10");

pub const INVALID_CURSOR_DEFINITION: SqlState = SqlState::new(*b"42P11");

pub const INVALID_DATABASE_DEFINITION: SqlState = SqlState::new(*b"42P12");

pub const INVALID_FUNCTION_DEFINITION: SqlState = SqlState::new(*b"42P13");

pub const INVALID_PREPARED_STATEMENT_DEFINITION: SqlState = SqlState::new(*b"42P14");

pub const INVALID_SCHEMA_DEFINITION: SqlState = SqlState::new(*b"42P15");

pub const INVALID_TABLE_DEFINITION: SqlState = SqlState::new(*b"42P16");

pub const INVALID_OBJECT_DEFINITION: SqlState = SqlState::new(*b"42P17");

pub const INDETERMINATE_DATATYPE: SqlState = SqlState::new(*b"42P18");

pub const INVALID_RECURSION: SqlState = SqlState::new(*b"42P19");

pub const WINDOWING_ERROR: SqlState = SqlState::new(*b"42P20");

pub const COLLATION_MISMATCH: SqlState = SqlState::new(*b"42P21");

pub const INDETERMINATE_COLLATION: SqlState = SqlState::new(*b"42P22");

pub const WITH_CHECK_OPTION_VIOLATION: SqlState = SqlState::new(*b"44000");

pub const INSUFFICIENT_RESOURCES: SqlState = SqlState::new(*b"53000");

pub const DISK_FULL: SqlState = SqlState::new(*b"53100");

pub const OUT_OF_MEMORY: SqlState = SqlState::new(*b"53200");

pub const TOO_MANY_CONNECTIONS: SqlState = SqlState::new(*b"53300");

pub const CONFIGURATION_LIMIT_EXCEEDED: SqlState = SqlState::new(*b"53400");

pub const PROGRAM_LIMIT_EXCEEDED: SqlState = SqlState::new(*b"54000");

pub const STATEMENT_TOO_COMPLEX: SqlState = SqlState::new(*b"54001");

pub const TOO_MANY_COLUMNS: SqlState = SqlState::new(*b"54011");

pub const TOO_MANY_ARGUMENTS: SqlState = SqlState::new(*b"54023");

pub const OBJECT_NOT_IN_PREREQUISITE_STATE: SqlState = SqlState::new(*b"55000");

pub const OBJECT_IN_USE: SqlState = SqlState::new(*b"55006");

pub const CANT_CHANGE_RUNTIME_PARAM: SqlState = SqlState::new(*b"55P02");

pub const LOCK_NOT_AVAILABLE: SqlState = SqlState::new(*b"55P03");

pub const OPERATOR_INTERVENTION: SqlState = SqlState::new(*b"57000");

pub const QUERY_CANCELED: SqlState = SqlState::new(*b"57014");

pub const ADMIN_SHUTDOWN: SqlState = SqlState::new(*b"57P01");

pub const CRASH_SHUTDOWN: SqlState = SqlState::new(*b"57P02");

pub const CANNOT_CONNECT_NOW: SqlState = SqlState::new(*b"57P03");

pub const DATABASE_DROPPED: SqlState = SqlState::new(*b"57P04");

pub const SYSTEM_ERROR: SqlState = SqlState::new(*b"58000");

pub const IO_ERROR: SqlState = SqlState::new(*b"58030");

pub const UNDEFINED_FILE: SqlState = SqlState::new(*b"58P01");

pub const DUPLICATE_FILE: SqlState = SqlState::new(*b"58P02");

pub const CONFIG_FILE_ERROR: SqlState = SqlState::new(*b"F0000");

pub const LOCK_FILE_EXISTS: SqlState = SqlState::new(*b"F0001");

pub const FDW_ERROR: SqlState = SqlState::new(*b"HV000");

pub const FDW_COLUMN_NAME_NOT_FOUND: SqlState = SqlState::new(*b"HV005");

pub const FDW_DYNAMIC_PARAMETER_VALUE_NEEDED: SqlState = SqlState::new(*b"HV002");

pub const FDW_FUNCTION_SEQUENCE_ERROR: SqlState = SqlState::new(*b"HV010");

pub const FDW_INCONSISTENT_DESCRIPTOR_INFORMATION: SqlState = SqlState::new(*b"HV021");

pub const FDW_INVALID_ATTRIBUTE_VALUE: SqlState = SqlState::new(*b"HV024");

pub const FDW_INVALID_COLUMN_NAME: SqlState = SqlState::new(*b"HV007");

pub const FDW_INVALID_COLUMN_NUMBER: SqlState = SqlState::new(*b"HV008");

pub const FDW_INVALID_DATA_TYPE: SqlState = SqlState::new(*b"HV004");

pub const FDW_INVALID_DATA_TYPE_DESCRIPTORS: SqlState = SqlState::new(*b"HV006");

pub const FDW_INVALID_DESCRIPTOR_FIELD_IDENTIFIER: SqlState = SqlState::new(*b"HV091");

pub const FDW_INVALID_HANDLE: SqlState = SqlState::new(*b"HV00B");

pub const FDW_INVALID_OPTION_INDEX: SqlState = SqlState::new(*b"HV00C");

pub const FDW_INVALID_OPTION_NAME: SqlState = SqlState::new(*b"HV00D");

pub const FDW_INVALID_STRING_LENGTH_OR_BUFFER_LENGTH: SqlState = SqlState::new(*b"HV090");

pub const FDW_INVALID_STRING_FORMAT: SqlState = SqlState::new(*b"HV00A");

pub const FDW_INVALID_USE_OF_NULL_POINTER: SqlState = SqlState::new(*b"HV009");

pub const FDW_TOO_MANY_HANDLES: SqlState = SqlState::new(*b"HV014");

pub const FDW_OUT_OF_MEMORY: SqlState = SqlState::new(*b"HV001");

pub const FDW_NO_SCHEMAS: SqlState = SqlState::new(*b"HV00P");

pub const FDW_OPTION_NAME_NOT_FOUND: SqlState = SqlState::new(*b"HV00J");

pub const FDW_REPLY_HANDLE: SqlState = SqlState::new(*b"HV00K");

pub const FDW_SCHEMA_NOT_FOUND: SqlState = SqlState::new(*b"HV00Q");

pub const FDW_TABLE_NOT_FOUND: SqlState = SqlState::new(*b"HV00R");

pub const FDW_UNABLE_TO_CREATE_EXECUTION: SqlState = SqlState::new(*b"HV00L");

pub const FDW_UNABLE_TO_CREATE_REPLY: SqlState = SqlState::new(*b"HV00M");

pub const FDW_UNABLE_TO_ESTABLISH_CONNECTION: SqlState = SqlState::new(*b"HV00N");

pub const PLPGSQL_ERROR: SqlState = SqlState::new(*b"P0000");

pub const RAISE_EXCEPTION: SqlState = SqlState::new(*b"P0001");

pub const NO_DATA_FOUND: SqlState = SqlState::new(*b"P0002");

pub const TOO_MANY_ROWS: SqlState = SqlState::new(*b"P0003");

pub const INTERNAL_ERROR: SqlState = SqlState::new(*b"XX000");

pub const DATA_CORRUPTED: SqlState = SqlState::new(*b"XX001");

pub const INDEX_CORRUPTED: SqlState = SqlState::new(*b"XX002");
