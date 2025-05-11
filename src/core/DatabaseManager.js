// src/core/DatabaseManager.js
const mysql = require('mysql2/promise');
const { Client } = require('pg');
const { MongoClient } = require('mongodb');
const snowflake = require('snowflake-sdk');

const MySQLAdapter = require('../adapters/MySQLAdapter');
const PostgreSQLAdapter = require('../adapters/PostgreSQLAdapter');
const MongoDBAdapter = require('../adapters/MongoDBAdapter');
const SnowflakeAdapter = require('../adapters/SnowflakeAdapter');

class DatabaseManager {
    constructor() {
        this.adapters = {
            mysql: new MySQLAdapter(),
            postgresql: new PostgreSQLAdapter(),
            mongodb: new MongoDBAdapter(),
            snowflake: new SnowflakeAdapter()
        };
        
        this.pools = new Map();
    }

    async createConnection(config) {
        const { type, ...connectionConfig } = config;
        
        if (!this.adapters[type]) {
            throw new Error(`Database type ${type} is not supported`);
        }

        // Check if connection pool already exists
        const poolKey = this._generatePoolKey(config);
        if (this.pools.has(poolKey)) {
            return this.pools.get(poolKey);
        }

        let connection;
        
        switch (type) {
            case 'mysql':
                connection = await this._createMySQLConnection(connectionConfig);
                break;
            case 'postgresql':
                connection = await this._createPostgreSQLConnection(connectionConfig);
                break;
            case 'mongodb':
                connection = await this._createMongoDBConnection(connectionConfig);
                break;
            case 'snowflake':
                connection = await this._createSnowflakeConnection(connectionConfig);
                break;
            default:
                throw new Error(`Unsupported database type: ${type}`);
        }

        this.pools.set(poolKey, connection);
        return connection;
    }

    async _createMySQLConnection(config) {
        try {
            const poolConfig = {
                host: config.host || 'localhost',
                port: config.port || 3306,
                user: config.user,
                password: config.password,
                database: config.database,
                waitForConnections: true,
                connectionLimit: config.connectionLimit || 10,
                queueLimit: 0,
                connectTimeout: config.connectTimeout || 60000,
                acquireTimeout: config.acquireTimeout || 60000,
                timeout: config.timeout || 60000,
                ...(config.ssl && { ssl: config.ssl })
            };

            const pool = mysql.createPool(poolConfig);
            
            // Test the connection
            const connection = await pool.getConnection();
            await connection.ping();
            connection.release();
            
            return { pool, type: 'mysql' };
        } catch (error) {
            console.error('MySQL connection error:', error);
            throw error;
        }
    }

    async _createPostgreSQLConnection(config) {
        try {
            const poolConfig = {
                host: config.host || 'localhost',
                port: config.port || 5432,
                user: config.user,
                password: config.password,
                database: config.database,
                max: config.max || 10,
                idleTimeoutMillis: config.idleTimeoutMillis || 30000,
                connectionTimeoutMillis: config.connectionTimeoutMillis || 60000,
                ...(config.ssl && { ssl: config.ssl })
            };

            const { Pool } = require('pg');
            const pool = new Pool(poolConfig);
            
            // Test the connection
            const client = await pool.connect();
            await client.query('SELECT NOW()');
            client.release();
            
            return { pool, type: 'postgresql' };
        } catch (error) {
            console.error('PostgreSQL connection error:', error);
            throw error;
        }
    }

    async _createMongoDBConnection(config) {
        try {
            const uri = config.uri || `mongodb://${config.user}:${config.password}@${config.host}:${config.port || 27017}/${config.database}`;
            
            const clientOptions = {
                maxPoolSize: config.maxPoolSize || 10,
                minPoolSize: config.minPoolSize || 0,
                serverSelectionTimeoutMS: config.serverSelectionTimeoutMS || 30000,
                socketTimeoutMS: config.socketTimeoutMS || 30000,
                ...(config.ssl && { ssl: config.ssl }),
                ...(config.authSource && { authSource: config.authSource })
            };

            const client = new MongoClient(uri, clientOptions);
            await client.connect();
            
            // Test the connection
            await client.db(config.database).admin().ping();
            
            return { client, database: config.database, type: 'mongodb' };
        } catch (error) {
            console.error('MongoDB connection error:', error);
            throw error;
        }
    }

    async _createSnowflakeConnection(config) {
        try {
            const connectionConfig = {
                account: config.account,
                username: config.user,
                password: config.password,
                warehouse: config.warehouse,
                database: config.database,
                schema: config.schema || 'PUBLIC',
                sessionParameters: {
                    QUERY_TIMEOUT: config.queryTimeout || 120000,
                    ...(config.sessionParameters || {})
                },
                streamResult: config.streamResult || false,
                complete: (err, connection) => {
                    if (err) {
                        throw err;
                    }
                }
            };

            const connection = snowflake.createConnection(connectionConfig);
            
            // Promisify the connect function
            await new Promise((resolve, reject) => {
                connection.connect((err, conn) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(conn);
                    }
                });
            });
            
            return { connection, type: 'snowflake' };
        } catch (error) {
            console.error('Snowflake connection error:', error);
            throw error;
        }
    }

    getAdapter(type) {
        const adapter = this.adapters[type];
        if (!adapter) {
            throw new Error(`Adapter for database type ${type} not found`);
        }
        return adapter;
    }

    _generatePoolKey(config) {
        const { host, port, database, user, type } = config;
        return `${type}://${user}@${host}:${port}/${database}`;
    }

    async closeConnection(poolKey) {
        const connection = this.pools.get(poolKey);
        if (!connection) {
            return;
        }

        try {
            switch (connection.type) {
                case 'mysql':
                    await connection.pool.end();
                    break;
                case 'postgresql':
                    await connection.pool.end();
                    break;
                case 'mongodb':
                    await connection.client.close();
                    break;
                case 'snowflake':
                    connection.connection.destroy();
                    break;
            }
            
            this.pools.delete(poolKey);
        } catch (error) {
            console.error(`Error closing connection for ${poolKey}:`, error);
            throw error;
        }
    }

    async closeAllConnections() {
        const promises = Array.from(this.pools.keys()).map(poolKey => 
            this.closeConnection(poolKey)
        );
        
        await Promise.allSettled(promises);
        this.pools.clear();
    }
}

module.exports = DatabaseManager;