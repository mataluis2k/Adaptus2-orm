// src/core/SchemaManager.js
const DataTypeMapper = require('../utils/DataTypeMapper');

class SchemaManager {
    constructor(databaseManager) {
        this.databaseManager = databaseManager;
        this.dataTypeMapper = new DataTypeMapper();
    }

    async createTable(connection, databaseType, tableName, schema) {
        try {
            const adapter = this.databaseManager.getAdapter(databaseType);
            
            switch (databaseType) {
                case 'mysql':
                case 'postgresql':
                case 'snowflake':
                    return await this._createSQLTable(connection, databaseType, tableName, schema, adapter);
                case 'mongodb':
                    return await this._createMongoCollection(connection, tableName, schema, adapter);
                default:
                    throw new Error(`Unsupported database type: ${databaseType}`);
            }
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async _createSQLTable(connection, databaseType, tableName, schema, adapter) {
        const columns = [];
        const constraints = [];
        
        for (const [columnName, columnDef] of Object.entries(schema.columns)) {
            const dataType = this.dataTypeMapper.mapType(columnDef.type, databaseType);
            let columnDDL = `"${columnName}" ${dataType}`;
            
            // Handle constraints
            if (columnDef.required) {
                columnDDL += ' NOT NULL';
            }
            
            if (columnDef.primaryKey) {
                constraints.push(`PRIMARY KEY ("${columnName}")`);
            }
            
            if (columnDef.unique) {
                constraints.push(`UNIQUE ("${columnName}")`);
            }
            
            if (columnDef.default !== undefined) {
                columnDDL += ` DEFAULT ${this._formatDefaultValue(columnDef.default, columnDef.type)}`;
            }
            
            columns.push(columnDDL);
        }
        
        // Handle indexes
        if (schema.indexes) {
            for (const [indexName, indexDef] of Object.entries(schema.indexes)) {
                const indexColumns = Array.isArray(indexDef.columns) 
                    ? indexDef.columns.map(col => `"${col}"`).join(',')
                    : `"${indexDef.columns}"`;
                    
                if (indexDef.unique) {
                    constraints.push(`UNIQUE INDEX "${indexName}" (${indexColumns})`);
                } else {
                    constraints.push(`INDEX "${indexName}" (${indexColumns})`);
                }
            }
        }
        
        // Handle foreign keys
        if (schema.foreignKeys) {
            for (const fk of schema.foreignKeys) {
                const fkDDL = `FOREIGN KEY ("${fk.column}") REFERENCES "${fk.references.table}"("${fk.references.column}")`;
                if (fk.onDelete) {
                    fkDDL += ` ON DELETE ${fk.onDelete}`;
                }
                if (fk.onUpdate) {
                    fkDDL += ` ON UPDATE ${fk.onUpdate}`;
                }
                constraints.push(fkDDL);
            }
        }
        
        const allDDL = [...columns, ...constraints];
        const createTableSQL = `CREATE TABLE "${tableName}" (${allDDL.join(',\n')})`;
        
        const result = await adapter.query(connection, createTableSQL);
        
        return {
            success: result.success,
            data: result.success ? 'Table created successfully' : null,
            error: result.error
        };
    }

    async _createMongoCollection(connection, collectionName, schema, adapter) {
        // For MongoDB, we create the collection and optionally set up indexes
        const result = await adapter.createCollection(connection, collectionName);
        
        if (!result.success) {
            return result;
        }
        
        // Create indexes if specified
        if (schema.indexes) {
            for (const [indexName, indexDef] of Object.entries(schema.indexes)) {
                const indexSpec = {};
                const indexOptions = { name: indexName };
                
                if (Array.isArray(indexDef.columns)) {
                    indexDef.columns.forEach(column => {
                        indexSpec[column] = 1; // 1 for ascending, -1 for descending
                    });
                } else {
                    indexSpec[indexDef.columns] = 1;
                }
                
                if (indexDef.unique) {
                    indexOptions.unique = true;
                }
                
                const indexResult = await adapter.createIndex(connection, collectionName, indexSpec, indexOptions);
                
                if (!indexResult.success) {
                    console.warn(`Failed to create index ${indexName}:`, indexResult.error);
                }
            }
        }
        
        return {
            success: true,
            data: 'Collection created successfully'
        };
    }

    async tableExists(connection, databaseType, tableName) {
        try {
            const adapter = this.databaseManager.getAdapter(databaseType);
            
            let query;
            switch (databaseType) {
                case 'mysql':
                    query = `
                        SELECT COUNT(*) as count
                        FROM information_schema.tables
                        WHERE table_schema = DATABASE()
                        AND table_name = ?
                    `;
                    break;
                case 'postgresql':
                    query = `
                        SELECT COUNT(*) as count
                        FROM information_schema.tables
                        WHERE table_schema = current_schema()
                        AND table_name = $1
                    `;
                    break;
                case 'snowflake':
                    query = `
                        SELECT COUNT(*) as count
                        FROM information_schema.tables
                        WHERE table_schema = CURRENT_SCHEMA()
                        AND table_name = :1
                    `;
                    break;
                case 'mongodb':
                    // For MongoDB, we check if collection exists
                    const db = connection.client.db(connection.database);
                    const collections = await db.listCollections({ name: tableName }).toArray();
                    return {
                        success: true,
                        data: collections.length > 0
                    };
                default:
                    throw new Error(`Unsupported database type: ${databaseType}`);
            }
            
            const result = await adapter.query(connection, query, [tableName]);
            
            if (result.success) {
                const count = result.data[0].count || result.data[0].COUNT;
                return {
                    success: true,
                    data: count > 0
                };
            }
            
            return result;
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async describeTable(connection, databaseType, tableName) {
        try {
            const adapter = this.databaseManager.getAdapter(databaseType);
            
            let query;
            switch (databaseType) {
                case 'mysql':
                    query = `DESCRIBE ${tableName}`;
                    break;
                case 'postgresql':
                    query = `
                        SELECT 
                            column_name, 
                            data_type, 
                            is_nullable, 
                            column_default
                        FROM information_schema.columns
                        WHERE table_schema = current_schema()
                        AND table_name = $1
                    `;
                    break;
                case 'snowflake':
                    query = `
                        SELECT 
                            column_name, 
                            data_type, 
                            is_nullable, 
                            column_default
                        FROM information_schema.columns
                        WHERE table_schema = CURRENT_SCHEMA()
                        AND table_name = :1
                    `;
                    break;
                case 'mongodb':
                    // For MongoDB, we return collection stats
                    const db = connection.client.db(connection.database);
                    const stats = await db.collection(tableName).stats();
                    return {
                        success: true,
                        data: stats
                    };
                default:
                    throw new Error(`Unsupported database type: ${databaseType}`);
            }
            
            const result = await adapter.query(connection, query, databaseType !== 'mysql' ? [tableName] : []);
            
            return result;
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async addColumn(connection, databaseType, tableName, columnName, columnDef) {
        try {
            const adapter = this.databaseManager.getAdapter(databaseType);
            
            if (databaseType === 'mongodb') {
                // MongoDB is schema-less, so no need to add columns
                return {
                    success: true,
                    data: 'MongoDB is schema-less, no column addition needed'
                };
            }
            
            const dataType = this.dataTypeMapper.mapType(columnDef.type, databaseType);
            let alterSQL = `ALTER TABLE "${tableName}" ADD COLUMN "${columnName}" ${dataType}`;
            
            if (columnDef.required) {
                alterSQL += ' NOT NULL';
            }
            
            if (columnDef.default !== undefined) {
                alterSQL += ` DEFAULT ${this._formatDefaultValue(columnDef.default, columnDef.type)}`;
            }
            
            const result = await adapter.query(connection, alterSQL);
            
            return result;
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async dropColumn(connection, databaseType, tableName, columnName) {
        try {
            const adapter = this.databaseManager.getAdapter(databaseType);
            
            if (databaseType === 'mongodb') {
                // For MongoDB, we need to remove the field from all documents
                const updateResult = await adapter.update(connection, tableName, {
                    where: {},
                    data: { $unset: { [columnName]: "" } }
                });
                
                return updateResult;
            }
            
            const alterSQL = `ALTER TABLE "${tableName}" DROP COLUMN "${columnName}"`;
            const result = await adapter.query(connection, alterSQL);
            
            return result;
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async createIndex(connection, databaseType, tableName, indexName, indexDef) {
        try {
            const adapter = this.databaseManager.getAdapter(databaseType);
            
            if (databaseType === 'mongodb') {
                const indexSpec = {};
                const indexOptions = { name: indexName };
                
                if (Array.isArray(indexDef.columns)) {
                    indexDef.columns.forEach(column => {
                        indexSpec[column] = 1;
                    });
                } else {
                    indexSpec[indexDef.columns] = 1;
                }
                
                if (indexDef.unique) {
                    indexOptions.unique = true;
                }
                
                return await adapter.createIndex(connection, tableName, indexSpec, indexOptions);
            }
            
            // For SQL databases
            const indexColumns = Array.isArray(indexDef.columns) 
                ? indexDef.columns.map(col => `"${col}"`).join(',')
                : `"${indexDef.columns}"`;
                
            let createIndexSQL = `CREATE ${indexDef.unique ? 'UNIQUE ' : ''}INDEX "${indexName}" ON "${tableName}" (${indexColumns})`;
            
            const result = await adapter.query(connection, createIndexSQL);
            
            return result;
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    _formatDefaultValue(value, type) {
        if (value === null) {
            return 'NULL';
        }
        
        switch (type) {
            case 'string':
                return `'${value.replace(/'/g, "''")}'`;
            case 'boolean':
                return value ? 'TRUE' : 'FALSE';
            case 'number':
            case 'integer':
            case 'float':
                return value;
            case 'date':
            case 'datetime':
            case 'timestamp':
                return `'${value}'`;
            default:
                return `'${value}'`;
        }
    }
}

module.exports = SchemaManager;