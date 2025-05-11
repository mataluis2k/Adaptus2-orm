
class DataTypeMapper {
    constructor() {
        this.typeMapping = {
            mysql: {
                string: 'VARCHAR',
                text: 'TEXT',
                longtext: 'LONGTEXT',
                integer: 'INT',
                biginteger: 'BIGINT',
                smallinteger: 'SMALLINT',
                decimal: 'DECIMAL',
                float: 'FLOAT',
                double: 'DOUBLE',
                boolean: 'BOOLEAN',
                date: 'DATE',
                datetime: 'DATETIME',
                timestamp: 'TIMESTAMP',
                time: 'TIME',
                year: 'YEAR',
                json: 'JSON',
                uuid: 'CHAR(36)',
                binary: 'BLOB',
                enum: 'ENUM'
            },
            postgresql: {
                string: 'VARCHAR',
                text: 'TEXT',
                longtext: 'TEXT',
                integer: 'INTEGER',
                biginteger: 'BIGINT',
                smallinteger: 'SMALLINT',
                decimal: 'DECIMAL',
                float: 'REAL',
                double: 'DOUBLE PRECISION',
                boolean: 'BOOLEAN',
                date: 'DATE',
                datetime: 'TIMESTAMP',
                timestamp: 'TIMESTAMP WITH TIME ZONE',
                time: 'TIME',
                year: 'INTEGER',
                json: 'JSONB',
                uuid: 'UUID',
                binary: 'BYTEA',
                enum: 'USER-DEFINED'
            },
            snowflake: {
                string: 'VARCHAR',
                text: 'VARCHAR',
                longtext: 'VARCHAR',
                integer: 'NUMBER(38,0)',
                biginteger: 'NUMBER(19,0)',
                smallinteger: 'NUMBER(5,0)',
                decimal: 'NUMBER',
                float: 'FLOAT',
                double: 'FLOAT',
                boolean: 'BOOLEAN',
                date: 'DATE',
                datetime: 'TIMESTAMP_NTZ',
                timestamp: 'TIMESTAMP_TZ',
                time: 'TIME',
                year: 'NUMBER(4,0)',
                json: 'VARIANT',
                uuid: 'VARCHAR(36)',
                binary: 'BINARY',
                enum: 'VARCHAR'
            },
            mongodb: {
                // MongoDB is schema-less, but we define these for validation purposes
                string: 'String',
                text: 'String',
                longtext: 'String',
                integer: 'Int32',
                biginteger: 'Int64',
                smallinteger: 'Int32',
                decimal: 'Decimal128',
                float: 'Double',
                double: 'Double',
                boolean: 'Boolean',
                date: 'Date',
                datetime: 'Date',
                timestamp: 'Date',
                time: 'String',
                year: 'Int32',
                json: 'Object',
                uuid: 'String',
                binary: 'BinData',
                enum: 'String',
                objectid: 'ObjectId',
                array: 'Array',
                object: 'Object'
            }
        };

        // Type options for specific databases
        this.typeOptions = {
            mysql: {
                string: {
                    length: 255
                },
                decimal: {
                    precision: 10,
                    scale: 2
                },
                enum: {
                    values: []
                }
            },
            postgresql: {
                string: {
                    length: 255
                },
                decimal: {
                    precision: 10,
                    scale: 2
                },
                enum: {
                    values: []
                }
            },
            snowflake: {
                string: {
                    length: 16777216 // Max length in Snowflake
                },
                decimal: {
                    precision: 38,
                    scale: 0
                }
            },
            mongodb: {
                // MongoDB doesn't have length restrictions like SQL databases
            }
        };
    }

    mapType(type, databaseType, options = {}) {
        databaseType = databaseType.toLowerCase();
        
        if (!this.typeMapping[databaseType]) {
            throw new Error(`Unsupported database type: ${databaseType}`);
        }

        const baseType = this.typeMapping[databaseType][type];
        
        if (!baseType) {
            throw new Error(`Unsupported type '${type}' for database '${databaseType}'`);
        }

        // Apply type-specific options
        return this._applyTypeOptions(baseType, type, databaseType, options);
    }

    _applyTypeOptions(baseType, type, databaseType, options) {
        const defaultOptions = this.typeOptions[databaseType]?.[type] || {};
        const mergedOptions = { ...defaultOptions, ...options };

        switch (databaseType) {
            case 'mysql':
                return this._applyMySQLOptions(baseType, type, mergedOptions);
            case 'postgresql':
                return this._applyPostgreSQLOptions(baseType, type, mergedOptions);
            case 'snowflake':
                return this._applySnowflakeOptions(baseType, type, mergedOptions);
            case 'mongodb':
                return baseType; // MongoDB doesn't need SQL-style options
            default:
                return baseType;
        }
    }

    _applyMySQLOptions(baseType, type, options) {
        switch (type) {
            case 'string':
                return `${baseType}(${options.length || 255})`;
            case 'decimal':
                return `${baseType}(${options.precision || 10},${options.scale || 2})`;
            case 'enum':
                if (options.values && options.values.length > 0) {
                    const values = options.values.map(v => `'${v}'`).join(',');
                    return `${baseType}(${values})`;
                }
                return baseType;
            default:
                return baseType;
        }
    }

    _applyPostgreSQLOptions(baseType, type, options) {
        switch (type) {
            case 'string':
                return `${baseType}(${options.length || 255})`;
            case 'decimal':
                return `NUMERIC(${options.precision || 10},${options.scale || 2})`;
            case 'enum':
                if (options.values && options.values.length > 0) {
                    // PostgreSQL requires creating a custom enum type first
                    // This is handled in the schema manager
                    return options.enumName || `${type}_enum`;
                }
                return baseType;
            default:
                return baseType;
        }
    }

    _applySnowflakeOptions(baseType, type, options) {
        switch (type) {
            case 'string':
                return `${baseType}(${options.length || 255})`;
            case 'decimal':
                return `${baseType}(${options.precision || 38},${options.scale || 0})`;
            case 'enum':
                // Snowflake doesn't have native enum, use CHECK constraint
                if (options.values && options.values.length > 0) {
                    return `${baseType}(255)`;
                }
                return baseType;
            default:
                return baseType;
        }
    }

    // Utility method to convert between different database types
    convertType(fromType, fromDatabase, toDatabase) {
        fromDatabase = fromDatabase.toLowerCase();
        toDatabase = toDatabase.toLowerCase();

        if (!this.typeMapping[fromDatabase] || !this.typeMapping[toDatabase]) {
            throw new Error('Unsupported database type for conversion');
        }

        // Find the generic type based on the source database type
        let genericType = null;
        for (const [generic, mapped] of Object.entries(this.typeMapping[fromDatabase])) {
            if (mapped === fromType) {
                genericType = generic;
                break;
            }
        }

        if (!genericType) {
            throw new Error(`Cannot find generic type for '${fromType}' in '${fromDatabase}'`);
        }

        // Map to target database type
        const targetType = this.typeMapping[toDatabase][genericType];
        if (!targetType) {
            throw new Error(`Cannot map '${genericType}' to '${toDatabase}'`);
        }

        return targetType;
    }

    // Validation method for type options
    validateTypeOptions(type, databaseType, options) {
        const validOptions = [];
        const defaultOptions = this.typeOptions[databaseType]?.[type] || {};

        switch (type) {
            case 'string':
                validOptions.push('length');
                if (options.length && typeof options.length !== 'number') {
                    throw new Error('Length option must be a number');
                }
                break;
            case 'decimal':
                validOptions.push('precision', 'scale');
                if (options.precision && typeof options.precision !== 'number') {
                    throw new Error('Precision option must be a number');
                }
                if (options.scale && typeof options.scale !== 'number') {
                    throw new Error('Scale option must be a number');
                }
                break;
            case 'enum':
                validOptions.push('values');
                if (options.values && !Array.isArray(options.values)) {
                    throw new Error('Values option must be an array');
                }
                break;
        }

        // Check for invalid options
        for (const option in options) {
            if (!validOptions.includes(option) && !defaultOptions.hasOwnProperty(option)) {
                console.warn(`Invalid option '${option}' for type '${type}'`);
            }
        }

        return true;
    }

    // Get default value for a type
    getDefaultValue(type, databaseType) {
        const defaults = {
            string: '',
            text: '',
            longtext: '',
            integer: 0,
            biginteger: 0,
            smallinteger: 0,
            decimal: 0.00,
            float: 0.0,
            double: 0.0,
            boolean: false,
            date: null,
            datetime: null,
            timestamp: null,
            time: null,
            year: null,
            json: {},
            uuid: null,
            binary: null,
            enum: null
        };

        if (databaseType === 'mongodb') {
            const mongoDefaults = {
                objectid: null,
                array: [],
                object: {}
            };
            return mongoDefaults[type] || defaults[type];
        }

        return defaults[type];
    }

    // Check if a type supports specific features
    supportsFeature(type, feature, databaseType) {
        const features = {
            mysql: {
                string: ['length', 'default', 'nullable', 'index', 'unique'],
                integer: ['default', 'nullable', 'index', 'unique', 'autoIncrement'],
                decimal: ['precision', 'scale', 'default', 'nullable', 'index', 'unique'],
                boolean: ['default', 'nullable', 'index'],
                date: ['default', 'nullable', 'index', 'unique'],
                datetime: ['default', 'nullable', 'index', 'unique'],
                json: ['default', 'nullable'],
                enum: ['values', 'default', 'nullable', 'index']
            },
            postgresql: {
                string: ['length', 'default', 'nullable', 'index', 'unique'],
                integer: ['default', 'nullable', 'index', 'unique', 'serial'],
                decimal: ['precision', 'scale', 'default', 'nullable', 'index', 'unique'],
                boolean: ['default', 'nullable', 'index'],
                date: ['default', 'nullable', 'index', 'unique'],
                datetime: ['default', 'nullable', 'index', 'unique'],
                json: ['default', 'nullable', 'gin'],
                enum: ['values', 'default', 'nullable', 'index']
            },
            snowflake: {
                string: ['length', 'default', 'nullable'],
                integer: ['default', 'nullable', 'autoIncrement'],
                decimal: ['precision', 'scale', 'default', 'nullable'],
                boolean: ['default', 'nullable'],
                date: ['default', 'nullable'],
                datetime: ['default', 'nullable'],
                json: ['default', 'nullable']
            },
            mongodb: {
                string: ['default', 'required', 'index', 'unique'],
                integer: ['default', 'required', 'index', 'unique'],
                decimal: ['default', 'required', 'index', 'unique'],
                boolean: ['default', 'required'],
                date: ['default', 'required', 'index', 'unique'],
                json: ['default', 'required'],
                objectid: ['default', 'required', 'index', 'unique'],
                array: ['default', 'required'],
                object: ['default', 'required']
            }
        };

        databaseType = databaseType.toLowerCase();
        const dbFeatures = features[databaseType];
        
        if (!dbFeatures || !dbFeatures[type]) {
            return false;
        }

        return dbFeatures[type].includes(feature);
    }
}

module.exports = DataTypeMapper;