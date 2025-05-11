// src/utils/ConfigManager.js
const path = require('path');
const fs = require('fs');

class ConfigManager {
    constructor() {
        this.configurations = new Map();
        this.defaultConfigs = {
            mysql: {
                host: process.env.MYSQL_HOST || 'localhost',
                port: process.env.MYSQL_PORT || 3306,
                user: process.env.MYSQL_USER || 'root',
                password: process.env.MYSQL_PASSWORD || '',
                database: process.env.MYSQL_DATABASE || 'test',
                connectionLimit: process.env.MYSQL_CONNECTION_LIMIT || 10,
                connectTimeout: 60000,
                acquireTimeout: 60000,
                timeout: 60000,
                waitForConnections: true,
                queueLimit: 0
            },
            postgresql: {
                host: process.env.POSTGRES_HOST || 'localhost',
                port: process.env.POSTGRES_PORT || 5432,
                user: process.env.POSTGRES_USER || 'postgres',
                password: process.env.POSTGRES_PASSWORD || '',
                database: process.env.POSTGRES_DATABASE || 'test',
                max: process.env.POSTGRES_MAX_CONNECTIONS || 10,
                idleTimeoutMillis: 30000,
                connectionTimeoutMillis: 60000,
                query_timeout: 60000,
                application_name: 'nodejs-orm'
            },
            mongodb: {
                host: process.env.MONGODB_HOST || 'localhost',
                port: process.env.MONGODB_PORT || 27017,
                user: process.env.MONGODB_USER || '',
                password: process.env.MONGODB_PASSWORD || '',
                database: process.env.MONGODB_DATABASE || 'test',
                uri: process.env.MONGODB_URI || null,
                maxPoolSize: process.env.MONGODB_MAX_POOL_SIZE || 10,
                minPoolSize: 0,
                serverSelectionTimeoutMS: 30000,
                socketTimeoutMS: 30000,
                authSource: process.env.MONGODB_AUTH_SOURCE || null
            },
            snowflake: {
                account: process.env.SNOWFLAKE_ACCOUNT || '',
                user: process.env.SNOWFLAKE_USER || '',
                password: process.env.SNOWFLAKE_PASSWORD || '',
                warehouse: process.env.SNOWFLAKE_WAREHOUSE || '',
                database: process.env.SNOWFLAKE_DATABASE || '',
                schema: process.env.SNOWFLAKE_SCHEMA || 'PUBLIC',
                queryTimeout: 120000,
                sessionParameters: {
                    QUERY_TIMEOUT: 120000,
                    TRANSACTION_DEFAULT_ISOLATION_LEVEL: 'READ COMMITTED'
                }
            }
        };
    }

    // Load configuration from environment variables or config file
    loadConfig(configPath) {
        if (configPath && fs.existsSync(configPath)) {
            try {
                const config = require(path.resolve(configPath));
                
                // Merge with default configurations
                for (const [type, typeConfig] of Object.entries(config)) {
                    if (this.defaultConfigs[type]) {
                        this.defaultConfigs[type] = {
                            ...this.defaultConfigs[type],
                            ...typeConfig
                        };
                    }
                }
                
                return true;
            } catch (error) {
                console.error(`Error loading config file: ${error.message}`);
                return false;
            }
        }
        
        return false;
    }

    // Get configuration for a specific database type
    getConfig(type, overrides = {}) {
        const normalizedType = type.toLowerCase();
        const baseConfig = this.defaultConfigs[normalizedType];
        
        if (!baseConfig) {
            throw new Error(`Configuration for database type '${type}' not found`);
        }

        // Apply overrides
        const finalConfig = {
            ...baseConfig,
            ...overrides,
            type: normalizedType
        };

        // Validate configuration
        this.validateConfig(finalConfig);
        
        // Cache the configuration
        const configKey = this._generateConfigKey(finalConfig);
        this.configurations.set(configKey, finalConfig);
        
        return finalConfig;
    }

    // Validate configuration based on database type
    validateConfig(config) {
        const { type } = config;
        
        switch (type) {
            case 'mysql':
                this._validateMySQLConfig(config);
                break;
            case 'postgresql':
                this._validatePostgreSQLConfig(config);
                break;
            case 'mongodb':
                this._validateMongoDBConfig(config);
                break;
            case 'snowflake':
                this._validateSnowflakeConfig(config);
                break;
            default:
                throw new Error(`Unsupported database type: ${type}`);
        }
    }

    _validateMySQLConfig(config) {
        const required = ['host', 'user', 'database'];
        this._checkRequiredFields(config, required, 'MySQL');
        
        if (config.port && (config.port < 1 || config.port > 65535)) {
            throw new Error('MySQL port must be between 1 and 65535');
        }
        
        if (config.connectionLimit && config.connectionLimit < 1) {
            throw new Error('MySQL connectionLimit must be at least 1');
        }
    }

    _validatePostgreSQLConfig(config) {
        const required = ['host', 'user', 'database'];
        this._checkRequiredFields(config, required, 'PostgreSQL');
        
        if (config.port && (config.port < 1 || config.port > 65535)) {
            throw new Error('PostgreSQL port must be between 1 and 65535');
        }
        
        if (config.max && config.max < 1) {
            throw new Error('PostgreSQL max connections must be at least 1');
        }
    }

    _validateMongoDBConfig(config) {
        if (!config.uri) {
            const required = ['host', 'database'];
            this._checkRequiredFields(config, required, 'MongoDB');
            
            if (config.port && (config.port < 1 || config.port > 65535)) {
                throw new Error('MongoDB port must be between 1 and 65535');
            }
        }
        
        if (config.maxPoolSize && config.maxPoolSize < 1) {
            throw new Error('MongoDB maxPoolSize must be at least 1');
        }
    }

    _validateSnowflakeConfig(config) {
        const required = ['account', 'user', 'password', 'warehouse', 'database'];
        this._checkRequiredFields(config, required, 'Snowflake');
    }

    _checkRequiredFields(config, required, databaseType) {
        for (const field of required) {
            if (!config[field]) {
                throw new Error(`${databaseType} configuration requires '${field}' field`);
            }
        }
    }

    // Build connection string for databases that support it
    buildConnectionString(config) {
        const { type } = config;
        
        switch (type) {
            case 'mysql':
                return this._buildMySQLConnectionString(config);
            case 'postgresql':
                return this._buildPostgreSQLConnectionString(config);
            case 'mongodb':
                return this._buildMongoDBConnectionString(config);
            case 'snowflake':
                return this._buildSnowflakeConnectionString(config);
            default:
                throw new Error(`Connection string builder not implemented for ${type}`);
        }
    }

    _buildMySQLConnectionString(config) {
        const { user, password, host, port, database } = config;
        const auth = password ? `${user}:${password}` : user;
        return `mysql://${auth}@${host}:${port}/${database}`;
    }

    _buildPostgreSQLConnectionString(config) {
        const { user, password, host, port, database } = config;
        const auth = password ? `${user}:${password}` : user;
        return `postgresql://${auth}@${host}:${port}/${database}`;
    }

    _buildMongoDBConnectionString(config) {
        if (config.uri) {
            return config.uri;
        }
        
        const { user, password, host, port, database } = config;
        let auth = '';
        
        if (user) {
            auth = password ? `${user}:${password}@` : `${user}@`;
        }
        
        return `mongodb://${auth}${host}:${port}/${database}`;
    }

    _buildSnowflakeConnectionString(config) {
        const { account, user, password, warehouse, database, schema } = config;
        return `snowflake://${user}:${password}@${account}.snowflakecomputing.com/?warehouse=${warehouse}&database=${database}&schema=${schema}`;
    }

    // Create configuration from connection string
    parseConnectionString(connectionString) {
        const url = new URL(connectionString);
        const protocol = url.protocol.replace(':', '');
        let config = { type: protocol };
        
        // Extract common parts
        config.host = url.hostname;
        config.port = url.port ? parseInt(url.port) : undefined;
        config.user = url.username || undefined;
        config.password = url.password || undefined;
        config.database = url.pathname.slice(1); // Remove leading slash
        
        // Parse query parameters
        const params = new URLSearchParams(url.search);
        
        switch (protocol) {
            case 'mysql':
                config = { ...this.defaultConfigs.mysql, ...config };
                break;
            case 'postgresql':
                config = { ...this.defaultConfigs.postgresql, ...config };
                break;
            case 'mongodb':
                config = { ...this.defaultConfigs.mongodb, ...config, uri: connectionString };
                if (params.get('authSource')) {
                    config.authSource = params.get('authSource');
                }
                break;
            case 'snowflake':
                config = { ...this.defaultConfigs.snowflake, ...config };
                config.account = url.hostname.split('.')[0];
                if (params.get('warehouse')) {
                    config.warehouse = params.get('warehouse');
                }
                if (params.get('schema')) {
                    config.schema = params.get('schema');
                }
                break;
            default:
                throw new Error(`Unsupported protocol: ${protocol}`);
        }
        
        this.validateConfig(config);
        return config;
    }

    // Create multiple configurations for database environments
    createEnvironmentConfigs(environment = 'development') {
        const configs = {};
        
        for (const type of ['mysql', 'postgresql', 'mongodb', 'snowflake']) {
            try {
                // Try to get environment-specific config
                const envPrefix = environment.toUpperCase();
                const envConfig = this._getEnvironmentConfig(type, envPrefix);
                
                if (envConfig) {
                    configs[type] = this.getConfig(type, envConfig);
                }
            } catch (error) {
                console.warn(`Could not create ${type} config for ${environment}: ${error.message}`);
            }
        }
        
        return configs;
    }

    _getEnvironmentConfig(type, envPrefix) {
        const upperType = type.toUpperCase();
        const config = {};
        
        switch (type) {
            case 'mysql':
                config.host = process.env[`${envPrefix}_MYSQL_HOST`];
                config.port = process.env[`${envPrefix}_MYSQL_PORT`];
                config.user = process.env[`${envPrefix}_MYSQL_USER`];
                config.password = process.env[`${envPrefix}_MYSQL_PASSWORD`];
                config.database = process.env[`${envPrefix}_MYSQL_DATABASE`];
                break;
            case 'postgresql':
                config.host = process.env[`${envPrefix}_POSTGRES_HOST`];
                config.port = process.env[`${envPrefix}_POSTGRES_PORT`];
                config.user = process.env[`${envPrefix}_POSTGRES_USER`];
                config.password = process.env[`${envPrefix}_POSTGRES_PASSWORD`];
                config.database = process.env[`${envPrefix}_POSTGRES_DATABASE`];
                break;
            case 'mongodb':
                config.uri = process.env[`${envPrefix}_MONGODB_URI`];
                config.host = process.env[`${envPrefix}_MONGODB_HOST`];
                config.port = process.env[`${envPrefix}_MONGODB_PORT`];
                config.user = process.env[`${envPrefix}_MONGODB_USER`];
                config.password = process.env[`${envPrefix}_MONGODB_PASSWORD`];
                config.database = process.env[`${envPrefix}_MONGODB_DATABASE`];
                break;
            case 'snowflake':
                config.account = process.env[`${envPrefix}_SNOWFLAKE_ACCOUNT`];
                config.user = process.env[`${envPrefix}_SNOWFLAKE_USER`];
                config.password = process.env[`${envPrefix}_SNOWFLAKE_PASSWORD`];
                config.warehouse = process.env[`${envPrefix}_SNOWFLAKE_WAREHOUSE`];
                config.database = process.env[`${envPrefix}_SNOWFLAKE_DATABASE`];
                config.schema = process.env[`${envPrefix}_SNOWFLAKE_SCHEMA`];
                break;
        }
        
        // Remove undefined values
        Object.keys(config).forEach(key => {
            if (config[key] === undefined) {
                delete config[key];
            }
        });
        
        return Object.keys(config).length > 0 ? config : null;
    }

    _generateConfigKey(config) {
        const { type, host, port, database, user } = config;
        return `${type}://${user}@${host}:${port}/${database}`;
    }

    // Save configuration to file
    saveConfig(config, filePath) {
        try {
            const existingConfig = fs.existsSync(filePath) ? require(path.resolve(filePath)) : {};
            
            existingConfig[config.type] = {
                ...existingConfig[config.type],
                ...this._sanitizeConfig(config)
            };
            
            fs.writeFileSync(filePath, JSON.stringify(existingConfig, null, 2));
            return true;
        } catch (error) {
            console.error(`Error saving config to file: ${error.message}`);
            return false;
        }
    }

    _sanitizeConfig(config) {
        // Remove sensitive data when saving to file
        const sanitized = { ...config };
        delete sanitized.password;
        return sanitized;
    }

    // Get all cached configurations
    getAllCached() {
        return Array.from(this.configurations.entries()).map(([key, config]) => ({
            key,
            config
        }));
    }

    // Clear cached configurations
    clearCache() {
        this.configurations.clear();
    }
}

module.exports = ConfigManager;