// examples/advanced-usage.js
const ORM = require('../index');
const { QueryBuilder } = require('../src/core/QueryBuilder');
const { MigrationManager } = require('../src/core/MigrationManager');
const { ConfigManager } = require('../src/utils/ConfigManager');

async function runAdvancedExamples() {
    console.log('=== Advanced ORM Usage Examples ===\n');
    
    const configManager = new ConfigManager();
    
    // Example configurations for different databases
    const configs = {
        mysql: configManager.getConfig('mysql', {
            host: 'localhost',
            port: 3306,
            user: 'root',
            password: 'password',
            database: 'advanced_orm_demo'
        }),
        postgresql: configManager.getConfig('postgresql', {
            host: 'localhost',
            port: 5432,
            user: 'postgres',
            password: 'password',
            database: 'advanced_orm_demo'
        }),
        mongodb: configManager.getConfig('mongodb', {
            uri: 'mongodb://localhost:27017/advanced_orm_demo',
            database: 'advanced_orm_demo'
        }),
        snowflake: configManager.getConfig('snowflake', {
            account: 'your_account',
            user: 'your_user',
            password: 'your_password',
            warehouse: 'COMPUTE_WH',
            database: 'ADVANCED_ORM_DEMO',
            schema: 'PUBLIC'
        })
    };
    
    // Example 1: Advanced Query Building
    console.log('Example 1: Advanced Query Building\n');
    await advancedQueryBuilding(configs.mysql);
    
    // Example 2: Transaction Management
    console.log('\nExample 2: Transaction Management\n');
    await transactionManagement(configs.postgresql);
    
    // Example 3: Schema Management
    console.log('\nExample 3: Schema Management\n');
    await schemaManagement(configs.mysql);
    
    // Example 4: Migration System
    console.log('\nExample 4: Migration System\n');
    await migrationSystem(configs.mysql);
    
    // Example 5: Bulk Operations
    console.log('\nExample 5: Bulk Operations\n');
    await bulkOperations(configs.postgresql);
    
    // Example 6: Database-Specific Features
    console.log('\nExample 6: Database-Specific Features\n');
    await databaseSpecificFeatures(configs);
    
    // Example 7: Connection Pooling
    console.log('\nExample 7: Connection Pooling Management\n');
    await connectionPooling(configs.mysql);
    
    // Example 8: Error Handling & Recovery
    console.log('\nExample 8: Error Handling & Recovery\n');
    await errorHandlingExample(configs.mysql);
    
    // Cleanup
    console.log('\nCleaning up...');
    await ORM.cleanup();
    console.log('✅ Cleanup completed');
    
    console.log('\n=== Advanced Examples Completed ===');
}

// Advanced Query Building
async function advancedQueryBuilding(config) {
    try {
        // Create complex query with joins, subqueries, and grouping
        const queryBuilder = QueryBuilder.create(config.type)
            .table('orders')
            .select([
                'orders.id',
                'orders.total',
                'customers.name',
                'customers.email',
                'COUNT(order_items.id) as item_count'
            ])
            .join('customers', 'orders.customer_id', 'customers.id')
            .leftJoin('order_items', 'orders.id', 'order_items.order_id')
            .where('orders.status', 'completed')
            .whereIn('orders.created_at', ['>=', new Date(Date.now() - 30 * 24 * 60 * 60 * 1000)])
            .groupBy(['orders.id', 'customers.name', 'customers.email'])
            .having('COUNT(order_items.id)', '>', 0)
            .orderBy('orders.total', 'DESC')
            .limit(10);
        
        const { sql, params } = queryBuilder.toSQL();
        console.log('Generated SQL:');
        console.log(sql);
        console.log('Parameters:', params);
        
        // Execute the complex query
        const result = await ORM.query(config, sql, params);
        console.log('Query results:', result.data);
        
        // Dynamic query building based on conditions
        const dynamicQuery = buildDynamicQuery({
            table: 'products',
            filters: {
                category: 'electronics',
                priceRange: { min: 100, max: 500 },
                inStock: true,
                brand: ['Sony', 'Samsung', 'Apple']
            },
            sort: { field: 'price', direction: 'ASC' },
            pagination: { page: 2, pageSize: 20 }
        });
        
        console.log('Dynamic SQL:', dynamicQuery.sql);
        console.log('Dynamic Parameters:', dynamicQuery.params);
        
    } catch (error) {
        console.error('Query building error:', error);
    }
}

// Helper function for dynamic query building
function buildDynamicQuery(options) {
    let query = QueryBuilder.create('mysql')
        .table(options.table)
        .select('*');
    
    // Apply filters dynamically
    if (options.filters) {
        for (const [key, value] of Object.entries(options.filters)) {
            if (key === 'priceRange' && value.min && value.max) {
                query = query.where('price', '>=', value.min)
                            .where('price', '<=', value.max);
            } else if (Array.isArray(value)) {
                query = query.whereIn(key, value);
            } else if (typeof value === 'boolean') {
                query = query.where(key, value);
            } else {
                query = query.where(key, value);
            }
        }
    }
    
    // Apply sorting
    if (options.sort) {
        query = query.orderBy(options.sort.field, options.sort.direction);
    }
    
    // Apply pagination
    if (options.pagination) {
        const offset = (options.pagination.page - 1) * options.pagination.pageSize;
        query = query.limit(options.pagination.pageSize).offset(offset);
    }
    
    return query.toSQL();
}

// Transaction Management
async function transactionManagement(config) {
    try {
        const connection = await ORM.getDbConnection(config);
        const adapter = ORM.databaseManager.getAdapter(config.type);
        
        // Begin transaction
        const txResult = await adapter.beginTransaction(connection);
        console.log('Transaction started:', txResult.success);
        
        try {
            // Perform multiple operations in transaction
            // Transfer funds between accounts
            await ORM.update(config, 'accounts', {
                where: { id: 1 },
                data: { balance: { $dec: 100 } }
            });
            
            await ORM.update(config, 'accounts', {
                where: { id: 2 },
                data: { balance: { $inc: 100 } }
            });
            
            // Log the transaction
            await ORM.create(config, 'transaction_logs', {
                from_account: 1,
                to_account: 2,
                amount: 100,
                type: 'transfer',
                timestamp: new Date()
            });
            
            // Commit transaction
            await adapter.commit(connection);
            console.log('✅ Transaction committed successfully');
            
        } catch (error) {
            // Rollback on error
            await adapter.rollback(connection);
            console.error('❌ Transaction rolled back:', error);
            throw error;
        }
        
        // Nested transaction example (savepoints)
        await nestedTransactionExample(config);
        
    } catch (error) {
        console.error('Transaction management error:', error);
    }
}

// Nested Transaction (Savepoint) Example
async function nestedTransactionExample(config) {
    try {
        console.log('\nNested Transaction Example:');
        
        // This would use savepoints for databases that support them
        const connection = await ORM.getDbConnection(config);
        
        // Begin main transaction
        await ORM.query(config, 'BEGIN');
        
        try {
            // Create an order
            const order = await ORM.create(config, 'orders', {
                customer_id: 1,
                total: 0,
                status: 'pending'
            });
            
            console.log('Order created:', order.data);
            
            // Create savepoint
            await ORM.query(config, 'SAVEPOINT order_items');
            
            try {
                // Add order items
                const items = [
                    { order_id: order.data.insertId, product_id: 1, quantity: 2, price: 50 },
                    { order_id: order.data.insertId, product_id: 2, quantity: 1, price: 100 }
                ];
                
                for (const item of items) {
                    await ORM.create(config, 'order_items', item);
                }
                
                // Update order total
                const total = items.reduce((sum, item) => sum + (item.quantity * item.price), 0);
                await ORM.update(config, 'orders', {
                    where: { id: order.data.insertId },
                    data: { total, status: 'confirmed' }
                });
                
                // Release savepoint (commit nested)
                await ORM.query(config, 'RELEASE SAVEPOINT order_items');
                
            } catch (error) {
                // Rollback to savepoint
                await ORM.query(config, 'ROLLBACK TO SAVEPOINT order_items');
                console.error('Rolled back to savepoint');
                throw error;
            }
            
            // Commit main transaction
            await ORM.query(config, 'COMMIT');
            console.log('✅ Nested transaction completed');
            
        } catch (error) {
            await ORM.query(config, 'ROLLBACK');
            console.error('❌ Main transaction rolled back');
            throw error;
        }
        
    } catch (error) {
        console.error('Nested transaction error:', error);
    }
}

// Schema Management
async function schemaManagement(config) {
    try {
        // Create tables with complex relationships
        await createComplexSchema(config);
        
        // Schema introspection
        const tables = await introspectSchema(config);
        console.log('Database tables:', tables);
        
        // Schema modifications
        await modifySchema(config);
        
        // Index management
        await manageIndexes(config);
        
    } catch (error) {
        console.error('Schema management error:', error);
    }
}

// Create Complex Schema
async function createComplexSchema(config) {
    console.log('Creating complex schema...');
    
    // Users table
    await ORM.createTable(config, 'users', {
        columns: {
            id: { type: 'integer', primaryKey: true, autoIncrement: true },
            email: { type: 'string', required: true, unique: true },
            password: { type: 'string', required: true },
            profile: { type: 'json' },
            created_at: { type: 'timestamp', default: 'CURRENT_TIMESTAMP' },
            updated_at: { type: 'timestamp', default: 'CURRENT_TIMESTAMP' }
        },
        indexes: {
            idx_email: { columns: ['email'], unique: true },
            idx_created_at: { columns: ['created_at'] }
        }
    });
    
    // Posts table with relationships
    await ORM.createTable(config, 'posts', {
        columns: {
            id: { type: 'integer', primaryKey: true, autoIncrement: true },
            user_id: { type: 'integer', required: true },
            title: { type: 'string', required: true },
            content: { type: 'text' },
            status: { type: 'enum', values: ['draft', 'published', 'archived'] },
            tags: { type: 'json' },
            published_at: { type: 'timestamp' },
            created_at: { type: 'timestamp', default: 'CURRENT_TIMESTAMP' }
        },
        foreignKeys: [
            {
                column: 'user_id',
                references: { table: 'users', column: 'id' },
                onDelete: 'CASCADE',
                onUpdate: 'CASCADE'
            }
        ],
        indexes: {
            idx_user_id: { columns: ['user_id'] },
            idx_status: { columns: ['status'] },
            idx_published_at: { columns: ['published_at'] }
        }
    });
    
    console.log('✅ Complex schema created');
}

// Schema Introspection
async function introspectSchema(config) {
    console.log('Introspecting schema...');
    
    const tables = [];
    
    // Get all tables
    const tablesResult = await ORM.query(config, `
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = DATABASE()
        AND table_type = 'BASE TABLE'
    `);
    
    if (tablesResult.success) {
        for (const table of tablesResult.data) {
            const tableName = table.table_name || table.TABLE_NAME;
            
            // Get table structure
            const structure = await ORM.schemaManager.describeTable(connection, config.type, tableName);
            
            if (structure.success) {
                tables.push({
                    name: tableName,
                    columns: structure.data
                });
            }
        }
    }
    
    return tables;
}

// Modify Schema
async function modifySchema(config) {
    console.log('Modifying schema...');
    
    // Add new column
    await ORM.schemaManager.addColumn(await ORM.getDbConnection(config), config.type, 'users', 'phone', {
        type: 'string',
        nullable: true,
        length: 20
    });
    
    // Modify existing column
    await ORM.query(config, `
        ALTER TABLE posts 
        MODIFY COLUMN content LONGTEXT
    `);
    
    console.log('✅ Schema modified');
}

// Index Management
async function manageIndexes(config) {
    console.log('Managing indexes...');
    
    // Create composite index
    await ORM.schemaManager.createIndex(
        await ORM.getDbConnection(config), 
        config.type, 
        'posts', 
        'idx_user_status', 
        {
            columns: ['user_id', 'status'],
            unique: false
        }
    );
    
    // Create partial index (PostgreSQL)
    if (config.type === 'postgresql') {
        await ORM.query(config, `
            CREATE INDEX idx_published_posts 
            ON posts (published_at) 
            WHERE status = 'published'
        `);
    }
    
    console.log('✅ Indexes managed');
}

// Migration System
async function migrationSystem(config) {
    try {
        const migrationManager = new MigrationManager(ORM, {
            ...config,
            migrationsPath: './migrations/advanced_demo'
        });
        
        // Initialize migration system
        await migrationManager.initialize();
        console.log('Migration system initialized');
        
        // Create migrations programmatically
        await createAdvancedMigrations(migrationManager);
        
        // Run migrations
        const migrateResult = await migrationManager.migrate();
        console.log('Migration result:', migrateResult);
        
        // Show migration status
        const status = await migrationManager.status();
        console.log('\nMigration Status:');
        console.log('Total:', status.data.summary.total);
        console.log('Executed:', status.data.summary.executed);
        console.log('Pending:', status.data.summary.pending);
        
        // Demonstrate rollback
        const rollbackResult = await migrationManager.rollback();
        console.log('Rollback result:', rollbackResult);
        
    } catch (error) {
        console.error('Migration system error:', error);
    }
}

// Create Advanced Migrations
async function createAdvancedMigrations(migrationManager) {
    // Create initial schema migration
    await migrationManager.create('create_advanced_schema');
    
    // Create data migration
    await migrationManager.create('seed_initial_data');
    
    // Create transformation migration
    await migrationManager.create('transform_user_profiles');
    
    console.log('✅ Advanced migrations created');
}

// Bulk Operations
async function bulkOperations(config) {
    try {
        const adapter = ORM.databaseManager.getAdapter(config.type);
        const connection = await ORM.getDbConnection(config);
        
        // Bulk insert
        const bulkData = Array.from({ length: 1000 }, (_, i) => ({
            name: `Bulk User ${i}`,
            email: `bulk${i}@example.com`,
            created_at: new Date()
        }));
        
        console.time('Bulk Insert');
        const bulkInsertResult = await adapter.bulkInsert(connection, 'users', bulkData);
        console.timeEnd('Bulk Insert');
        console.log('Bulk insert result:', bulkInsertResult);
        
        // Bulk update using raw query
        console.time('Bulk Update');
        const bulkUpdateResult = await ORM.query(config, `
            UPDATE users 
            SET updated_at = NOW() 
            WHERE created_at > ?
        `, [new Date(Date.now() - 24 * 60 * 60 * 1000)]);
        console.timeEnd('Bulk Update');
        console.log('Bulk update result:', bulkUpdateResult);
        
        // Bulk upsert
        const upsertData = Array.from({ length: 100 }, (_, i) => ({
            id: i + 1,
            name: `Updated User ${i}`,
            email: `updated${i}@example.com`,
            profile: JSON.stringify({ plan: 'premium' })
        }));
        
        console.time('Bulk Upsert');
        const results = [];
        for (const item of upsertData) {
            const result = await adapter.upsert(
                connection, 
                'users', 
                item, 
                ['id'], 
                ['name', 'email', 'profile']
            );
            results.push(result);
        }
        console.timeEnd('Bulk Upsert');
        console.log('Bulk upsert completed:', results.length);
        
    } catch (error) {
        console.error('Bulk operations error:', error);
    }
}

// Database-Specific Features
async function databaseSpecificFeatures(configs) {
    try {
        // MySQL-specific features
        if (configs.mysql) {
            console.log('MySQL-specific features:');
            await mysqlSpecificFeatures(configs.mysql);
        }
        
        // PostgreSQL-specific features
        if (configs.postgresql) {
            console.log('\nPostgreSQL-specific features:');
            await postgresqlSpecificFeatures(configs.postgresql);
        }
        
        // MongoDB-specific features
        if (configs.mongodb) {
            console.log('\nMongoDB-specific features:');
            await mongodbSpecificFeatures(configs.mongodb);
        }
        
        // Snowflake-specific features
        if (configs.snowflake) {
            console.log('\nSnowflake-specific features:');
            await snowflakeSpecificFeatures(configs.snowflake);
        }
        
    } catch (error) {
        console.error('Database-specific features error:', error);
    }
}

// MySQL-specific features
async function mysqlSpecificFeatures(config) {
    try {
        const adapter = ORM.databaseManager.getAdapter(config.type);
        const connection = await ORM.getDbConnection(config);
        
        // JSON column operations
        await ORM.create(config, 'user_preferences', {
            user_id: 1,
            preferences: JSON.stringify({
                theme: 'dark',
                notifications: true,
                language: 'en'
            })
        });
        
        // Query JSON column
        const jsonQuery = await ORM.query(config, `
            SELECT * FROM user_preferences 
            WHERE JSON_EXTRACT(preferences, '$.theme') = 'dark'
        `);
        console.log('JSON query result:', jsonQuery.data);
        
        // Full-text search
        await ORM.query(config, `
            ALTER TABLE posts 
            ADD FULLTEXT(title, content)
        `);
        
        const fullTextResult = await ORM.query(config, `
            SELECT *, MATCH(title, content) AGAINST(? IN NATURAL LANGUAGE MODE) as score
            FROM posts
            WHERE MATCH(title, content) AGAINST(? IN NATURAL LANGUAGE MODE)
            ORDER BY score DESC
        `, ['javascript tutorial', 'javascript tutorial']);
        
        console.log('Full-text search result:', fullTextResult.data);
        
    } catch (error) {
        console.error('MySQL-specific error:', error);
    }
}

// PostgreSQL-specific features
async function postgresqlSpecificFeatures(config) {
    try {
        const adapter = ORM.databaseManager.getAdapter(config.type);
        const connection = await ORM.getDbConnection(config);
        
        // JSONB operations
        await adapter.jsonbQuery(connection, 'users', 'profile', '{"plan": "premium"}');
        
        // Array operations
        await ORM.create(config, 'products', {
            name: 'Multi-tool',
            tags: ['tools', 'hardware', 'outdoor'],
            categories: [1, 2, 3]
        });
        
        const arrayResult = await adapter.arrayContains(connection, 'products', 'tags', 'tools');
        console.log('Array query result:', arrayResult.data);
        
        // Full-text search with ranking
        const searchResult = await adapter.fullTextSearch(
            connection, 
            'posts', 
            'content', 
            'javascript & tutorial', 
            { ranked: true }
        );
        console.log('Full-text search with ranking:', searchResult.data);
        
        // Create extension
        await adapter.createExtension(connection, 'uuid-ossp');
        
        // Use UUID
        await ORM.query(config, `
            CREATE TABLE sessions (
                id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
                user_id INTEGER,
                created_at TIMESTAMP DEFAULT NOW()
            )
        `);
        
    } catch (error) {
        console.error('PostgreSQL-specific error:', error);
    }
}

// MongoDB-specific features
async function mongodbSpecificFeatures(config) {
    try {
        const adapter = ORM.databaseManager.getAdapter(config.type);
        const connection = await ORM.getDbConnection(config);
        
        // Aggregation pipeline
        const aggregationResult = await adapter.query(connection, [
            { $match: { status: 'active' } },
            { $group: { 
                _id: '$category', 
                count: { $sum: 1 },
                totalValue: { $sum: '$value' }
            }},
            { $sort: { totalValue: -1 } }
        ], { collection: 'products' });
        
        console.log('Aggregation result:', aggregationResult.data);
        
        // Create compound index
        await adapter.createIndex(connection, 'products', 
            { category: 1, status: 1, value: -1 }, 
            { name: 'idx_compound' }
        );
        
        // Text search
        await adapter.createIndex(connection, 'products', 
            { name: 'text', description: 'text' }, 
            { name: 'idx_text_search' }
        );
        
        const textSearchResult = await adapter.query(connection, [
            { $match: { $text: { $search: 'javascript tutorial' } } },
            { $addFields: { score: { $meta: 'textScore' } } },
            { $sort: { score: -1 } }
        ], { collection: 'products' });
        
        console.log('Text search result:', textSearchResult.data);
        
    } catch (error) {
        console.error('MongoDB-specific error:', error);
    }
}

// Snowflake-specific features
async function snowflakeSpecificFeatures(config) {
    try {
        const adapter = ORM.databaseManager.getAdapter(config.type);
        const connection = await ORM.getDbConnection(config);
        
        // Warehouse management
        await adapter.switchWarehouse(connection, 'ANALYTICS_WH');
        const currentWarehouse = await adapter.getCurrentWarehouse(connection);
        console.log('Current warehouse:', currentWarehouse.data);
        
        // Time travel
        const timeTravelResult = await adapter.queryTimeTravelBefore(
            connection, 
            'orders', 
            '2024-01-01 00:00:00'
        );
        console.log('Time travel result:', timeTravelResult.data);
        
        // Clone table
        await adapter.cloneTable(connection, 'users', 'users_backup');
        console.log('Table cloned successfully');
        
        // VARIANT operations
        await adapter.queryVariant(connection, 'events', 'data', 'user_id');
        
        // Bulk loading from stage
        await adapter.createStage(connection, 'my_stage', 's3://mybucket/data/', {
            aws_key_id: 'your_key',
            aws_secret_key: 'your_secret'
        });
        
        await adapter.copyInto(connection, 'bulk_data', 'my_stage', '*.csv', {
            fileFormat: '(TYPE = CSV)',
            onError: 'CONTINUE'
        });
        
        // Query history
        const queryHistory = await adapter.getQueryHistory(connection, {
            startTime: '2024-01-01',
            limit: 10
        });
        console.log('Query history:', queryHistory.data);
        
    } catch (error) {
        console.error('Snowflake-specific error:', error);
    }
}

// Connection Pooling
async function connectionPooling(config) {
    try {
        console.log('Connection pooling demo...');
        
        // Multiple simultaneous connections
        const connections = await Promise.all([
            ORM.getDbConnection(config),
            ORM.getDbConnection(config),
            ORM.getDbConnection(config),
            ORM.getDbConnection(config),
            ORM.getDbConnection(config)
        ]);
        
        console.log('Created 5 connections (should reuse pool)');
        
        // Perform concurrent operations
        const operations = connections.map(async (conn, index) => {
            return ORM.query(config, `SELECT ${index + 1} as connection_id`);
        });
        
        const results = await Promise.all(operations);
        console.log('Concurrent operation results:', results.map(r => r.data));
        
        // Check pool status (implementation specific)
        console.log('Pool connections active');
        
        // Close specific connections (if needed)
        await ORM.closeAllMysqlPools();
        console.log('MySQL pools closed');
        
    } catch (error) {
        console.error('Connection pooling error:', error);
    }
}

// Error Handling & Recovery
async function errorHandlingExample(config) {
    try {
        console.log('Error handling demo...');
        
        // Handling connection errors
        try {
            const badConfig = { ...config, password: 'wrong_password' };
            await ORM.getDbConnection(badConfig);
        } catch (error) {
            console.log('✅ Caught connection error:', error.message);
        }
        
        // Handling constraint violations
        try {
            await ORM.create(config, 'users', {
                email: 'duplicate@example.com',
                password: 'password'
            });
            
            // This should fail due to unique constraint
            await ORM.create(config, 'users', {
                email: 'duplicate@example.com',
                password: 'password'
            });
        } catch (error) {
            console.log('✅ Caught constraint violation:', error.message);
        }
        
        // Query timeout handling
        try {
            await ORM.query(config, 'SELECT SLEEP(10)'); // This might timeout
        } catch (error) {
            console.log('✅ Caught timeout error:', error.message);
        }
        
        // Transaction error handling
        const connection = await ORM.getDbConnection(config);
        const adapter = ORM.databaseManager.getAdapter(config.type);
        
        await adapter.beginTransaction(connection);
        
        try {
            await ORM.create(config, 'test_table', { name: 'Test' });
            
            // This will fail if column doesn't exist
            await ORM.create(config, 'test_table', { nonexistent_column: 'Value' });
            
            await adapter.commit(connection);
        } catch (error) {
            console.log('✅ Transaction error handled, rolling back...');
            await adapter.rollback(connection);
        }
        
    } catch (error) {
        console.error('Error handling example error:', error);
    }
}

// Run all examples
if (require.main === module) {
    runAdvancedExamples().catch(console.error);
}

module.exports = {
    runAdvancedExamples,
    advancedQueryBuilding,
    transactionManagement,
    schemaManagement,
    migrationSystem,
    bulkOperations,
    databaseSpecificFeatures,
    connectionPooling,
    errorHandlingExample
};