// index.js - Main ORM entry point
const path = require('path');
const DatabaseManager = require('./src/core/DatabaseManager');
const QueryBuilder = require('./src/core/QueryBuilder');
const SchemaManager = require('./src/core/SchemaManager');

// Static response object definition - no external dependency needed
const response = {
    statusCode: 200,
    message: '',
    error: '',
    data: {},
    source_action: '',
    
    setResponse(statusCode, message, error, data, source_action = '') {
        this.statusCode = statusCode;
        this.message = message;
        this.error = error;
        this.data = data;
        this.source_action = source_action;
        return this;
    },
    
    success(data, message = 'Success', source_action = '') {
        return this.setResponse(200, message, '', data, source_action);
    },
    
    error(message, error = '', source_action = '') {
        return this.setResponse(500, message, error, {}, source_action);
    },
    
    toJSON() {
        return {
            statusCode: this.statusCode,
            message: this.message,
            error: this.error,
            data: this.data,
            source_action: this.source_action
        };
    }
};

class ORM {
    constructor() {
        this.databaseManager = new DatabaseManager();
        this.queryBuilder = new QueryBuilder();
        this.schemaManager = new SchemaManager(this.databaseManager);
        this.connections = new Map();
    }

    async getDbConnection(config) {
        try {
            const connectionKey = this._generateConnectionKey(config);
            
            if (!this.connections.has(connectionKey)) {
                const connection = await this.databaseManager.createConnection(config);
                this.connections.set(connectionKey, connection);
                
                // Set up connection error handling
                if (connection.on) {
                    connection.on('error', (err) => {
                        console.error(`Database connection error for ${config.type}:`, err);
                        this.connections.delete(connectionKey);
                    });
                }
            }
            
            return this.connections.get(connectionKey);
        } catch (error) {
            console.error('Error getting database connection:', error);
            throw error;
        }
    }

    async create(config, table, params) {
        try {
            const connection = await this.getDbConnection(config);
            const adapter = this.databaseManager.getAdapter(config.type);
            
            const result = await adapter.create(connection, table, params);
            
            if (result.success) {
                return response.success(result.data, 'Record created successfully');
            }
            
            return response.error('Failed to create record', result.error);
        } catch (error) {
            console.error('Create error:', error);
            return response.error('Error creating record', error);
        }
    }

    async read(config, table, params = {}) {
        try {
            const connection = await this.getDbConnection(config);
            const adapter = this.databaseManager.getAdapter(config.type);
            
            const result = await adapter.read(connection, table, params);
            
            if (result.success) {
                return response.success(result.data, 'Records retrieved successfully');
            }
            
            return response.error('Failed to read records', result.error);
        } catch (error) {
            console.error('Read error:', error);
            return response.error('Error reading records', error);
        }
    }

    async update(config, table, params) {
        try {
            const connection = await this.getDbConnection(config);
            const adapter = this.databaseManager.getAdapter(config.type);
            
            const result = await adapter.update(connection, table, params);
            
            if (result.success) {
                return response.success(result.data, 'Record updated successfully');
            }
            
            return response.error('Failed to update record', result.error);
        } catch (error) {
            console.error('Update error:', error);
            return response.error('Error updating record', error);
        }
    }

    async deleteRecord(config, table, params) {
        try {
            const connection = await this.getDbConnection(config);
            const adapter = this.databaseManager.getAdapter(config.type);
            
            const result = await adapter.delete(connection, table, params);
            
            if (result.success) {
                return response.success(result.data, 'Record deleted successfully');
            }
            
            return response.error('Failed to delete record', result.error);
        } catch (error) {
            console.error('Delete error:', error);
            return response.error('Error deleting record', error);
        }
    }

    async exists(config, table, params) {
        try {
            const connection = await this.getDbConnection(config);
            const adapter = this.databaseManager.getAdapter(config.type);
            
            const result = await adapter.exists(connection, table, params);
            
            if (result.success) {
                return response.success(result.data, 'Existence check completed');
            }
            
            return response.error('Failed to check existence', result.error);
        } catch (error) {
            console.error('Exists error:', error);
            return response.error('Error checking existence', error);
        }
    }

    async createTable(config, table, schema) {
        try {
            const connection = await this.getDbConnection(config);
            const result = await this.schemaManager.createTable(connection, config.type, table, schema);
            
            if (result.success) {
                return response.success(null, 'Table created successfully');
            }
            
            return response.error('Failed to create table', result.error);
        } catch (error) {
            console.error('Create table error:', error);
            return response.error('Error creating table', error);
        }
    }

    async tableExists(config, table) {
        try {
            const connection = await this.getDbConnection(config);
            const result = await this.schemaManager.tableExists(connection, config.type, table);
            
            if (result.success) {
                return response.success(result.data, 'Table existence checked');
            }
            
            return response.error('Failed to check table existence', result.error);
        } catch (error) {
            console.error('Table exists error:', error);
            return response.error('Error checking table existence', error);
        }
    }

    async query(config, queryString, params = []) {
        try {
            const connection = await this.getDbConnection(config);
            const adapter = this.databaseManager.getAdapter(config.type);
            
            const result = await adapter.query(connection, queryString, params);
            
            if (result.success) {
                return response.success(result.data, 'Query executed successfully');
            }
            
            return response.error('Failed to execute query', result.error);
        } catch (error) {
            console.error('Query error:', error);
            return response.error('Error executing query', error);
        }
    }

    async initDatabase(config) {
        try {
            // Initialize the database connection
            const connection = await this.getDbConnection(config);
            const adapter = this.databaseManager.getAdapter(config.type);
            
            if (adapter.initDatabase) {
                const result = await adapter.initDatabase(connection, config);
                
                if (result.success) {
                    return response.success(null, 'Database initialized successfully');
                }
                
                return response.error('Failed to initialize database', result.error);
            }
            
            return response.success(null, 'Database connection established');
        } catch (error) {
            console.error('Init database error:', error);
            return response.error('Error initializing database', error);
        }
    }

    async closeAllMysqlPools() {
        try {
            const mysqlConnections = Array.from(this.connections.entries())
                .filter(([key]) => key.includes('mysql'));
            
            const results = [];
            for (const [key, connection] of mysqlConnections) {
                try {
                    if (connection.pool && typeof connection.pool.end === 'function') {
                        await connection.pool.end();
                        results.push({ key, success: true });
                    } else if (connection.close && typeof connection.close === 'function') {
                        await connection.close();
                        results.push({ key, success: true });
                    }
                    this.connections.delete(key);
                } catch (error) {
                    results.push({ key, success: false, error: error.message });
                }
            }
            
            return response.success(results, 'MySQL pools closed');
        } catch (error) {
            console.error('Close MySQL pools error:', error);
            return response.error('Error closing MySQL pools', error);
        }
    }

    async extendContext(config) {
        try {
            const { globalContext, getContext } = require('./context');
            
            // Initialize ORM methods in context
            const context = await getContext();
            
            context.db = {
                getConnection: this.getDbConnection.bind(this),
                create: this.create.bind(this),
                read: this.read.bind(this),
                update: this.update.bind(this),
                delete: this.deleteRecord.bind(this),
                exists: this.exists.bind(this),
                createTable: this.createTable.bind(this),
                tableExists: this.tableExists.bind(this),
                query: this.query.bind(this),
                initDatabase: this.initDatabase.bind(this),
                closeAllMysqlPools: this.closeAllMysqlPools.bind(this)
            };
            
            // Set up default configuration
            if (config) {
                context.db.config = config;
            }
            
            return response.success(context, 'Context extended successfully');
        } catch (error) {
            console.error('Extend context error:', error);
            return response.error('Error extending context', error);
        }
    }

    _generateConnectionKey(config) {
        const { host, port, database, user, type } = config;
        return `${type}://${user}@${host}:${port}/${database}`;
    }

    // Clean up connections on process exit
    async cleanup() {
        try {
            for (const [key, connection] of this.connections) {
                try {
                    if (connection.pool && typeof connection.pool.end === 'function') {
                        await connection.pool.end();
                    } else if (connection.close && typeof connection.close === 'function') {
                        await connection.close();
                    } else if (connection.destroy && typeof connection.destroy === 'function') {
                        await connection.destroy();
                    }
                } catch (error) {
                    console.error(`Error closing connection ${key}:`, error);
                }
            }
            this.connections.clear();
        } catch (error) {
            console.error('Cleanup error:', error);
        }
    }
}

// Set up cleanup handlers
const orm = new ORM();

process.on('exit', async () => {
    await orm.cleanup();
});

process.on('SIGINT', async () => {
    await orm.cleanup();
    process.exit(0);
});

process.on('uncaughtException', async (error) => {
    console.error('Uncaught exception:', error);
    await orm.cleanup();
    process.exit(1);
});

module.exports = new ORM();