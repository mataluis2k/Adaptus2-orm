// examples/basic-usage.js
const ORM = require('../index');
const { ConfigManager } = require('../src/utils/ConfigManager');

async function runExample() {
    console.log('=== Multi-Database ORM Example ===\n');
    
    // Create config manager
    const configManager = new ConfigManager();
    
    // Example configurations for different databases
    const configs = {
        mysql: configManager.getConfig('mysql', {
            host: 'localhost',
            port: 3306,
            user: 'root',
            password: 'password',
            database: 'test_db'
        }),
        postgresql: configManager.getConfig('postgresql', {
            host: 'localhost',
            port: 5432,
            user: 'postgres',
            password: 'password',
            database: 'test_db'
        })
    };
    
    // Example 1: Basic CRUD operations with MySQL
    console.log('Example 1: CRUD Operations with MySQL\n');
    
    try {
        // Create a connection
        const mysqlConnection = await ORM.getDbConnection(configs.mysql);
        console.log('✅ MySQL connection established');
        
        // Create a table
        const createTableResult = await ORM.createTable(configs.mysql, 'example_users', {
            columns: {
                id: { type: 'integer', primaryKey: true, autoIncrement: true },
                name: { type: 'string', required: true },
                email: { type: 'string', required: true, unique: true },
                age: { type: 'integer' },
                created_at: { type: 'timestamp', default: 'CURRENT_TIMESTAMP' }
            },
            indexes: {
                idx_email: { columns: ['email'], unique: true }
            }
        });
        
        if (createTableResult.success) {
            console.log('✅ Table created successfully');
        }
        
        // Insert some data
        const user1 = await ORM.create(configs.mysql, 'example_users', {
            name: 'John Doe',
            email: 'john@example.com',
            age: 30
        });
        
        const user2 = await ORM.create(configs.mysql, 'example_users', {
            name: 'Jane Smith',
            email: 'jane@example.com',
            age: 25
        });
        
        console.log('✅ Created users:', user1.data, user2.data);
        
        // Read data
        const allUsers = await ORM.read(configs.mysql, 'example_users', {
            orderBy: 'name'
        });
        
        console.log('✅ All users:', allUsers.data);
        
        // Update data
        const updateResult = await ORM.update(configs.mysql, 'example_users', {
            where: { id: user1.data.insertId },
            data: { age: 31 }
        });
        
        console.log('✅ Updated user:', updateResult.data);
        
        // Delete data
        const deleteResult = await ORM.deleteRecord(configs.mysql, 'example_users', {
            where: { id: user2.data.insertId }
        });
        
        console.log('✅ Deleted user:', deleteResult.data);
        
    } catch (error) {
        console.error('❌ MySQL Example Error:', error.message);
    }
    
    // Example 2: Query Builder
    console.log('\nExample 2: Query Builder\n');
    
    try {
        const { QueryBuilder } = require('../src/core/QueryBuilder');
        
        const queryBuilder = QueryBuilder.create('mysql')
            .table('example_users')
            .select(['id', 'name', 'email'])
            .where('age', '>=', 25)
            .orderBy('name', 'ASC')
            .limit(10);
        
        const { sql, params } = queryBuilder.toSQL();
        console.log('✅ Generated SQL:', sql);
        console.log('✅ Parameters:', params);
        
        const result = await ORM.query(configs.mysql, sql, params);
        console.log('✅ Query result:', result.data);
        
    } catch (error) {
        console.error('❌ Query Builder Error:', error.message);
    }
    
    // Example 3: MongoDB operations (if MongoDB is available)
    console.log('\nExample 3: MongoDB Operations\n');
    
    try {
        const mongoConfig = configManager.getConfig('mongodb', {
            uri: 'mongodb://localhost:27017/test_db',
            database: 'test_db'
        });
        
        // Note: This will only work if MongoDB is running and accessible
        const mongoConnection = await ORM.getDbConnection(mongoConfig);
        console.log('✅ MongoDB connection established');
        
        // Create a document
        const doc = await ORM.create(mongoConfig, 'example_collection', {
            name: 'MongoDB Document',
            type: 'example',
            tags: ['demo', 'orm'],
            metadata: {
                created: new Date(),
                version: 1
            }
        });
        
        console.log('✅ Created document:', doc.data);
        
        // Find documents
        const docs = await ORM.read(mongoConfig, 'example_collection', {
            where: { type: 'example' }
        });
        
        console.log('✅ Found documents:', docs.data);
        
    } catch (error) {
        console.error('❌ MongoDB Example Error (expected if not running):', error.message);
    }
    
    // Example 4: Transaction example
    console.log('\nExample 4: Transaction Example\n');
    
    try {
        const adapter = ORM.databaseManager.getAdapter(configs.mysql.type);
        const connection = await ORM.getDbConnection(configs.mysql);
        
        // Begin transaction
        const txResult = await adapter.beginTransaction(connection);
        
        if (txResult.success) {
            try {
                // Create order
                const order = await ORM.create(configs.mysql, 'example_orders', {
                    customer_id: 1,
                    total: 99.99,
                    status: 'pending'
                });
                
                // Update inventory (assuming table exists)
                await ORM.update(configs.mysql, 'example_inventory', {
                    where: { product_id: 1 },
                    data: { quantity: { $dec: 5 } }
                });
                
                // Commit transaction
                await adapter.commit(connection);
                console.log('✅ Transaction completed successfully');
                
            } catch (error) {
                // Rollback on error
                await adapter.rollback(connection);
                console.error('❌ Transaction rolled back:', error.message);
            }
        }
        
    } catch (error) {
        console.error('❌ Transaction Example Error:', error.message);
    }
    
    // Cleanup
    console.log('\nCleaning up...');
    await ORM.cleanup();
    console.log('✅ Cleanup completed');
    
    console.log('\n=== Example completed ===');
}

// Run the example
if (require.main === module) {
    runExample().catch(console.error);
}

module.exports = runExample;