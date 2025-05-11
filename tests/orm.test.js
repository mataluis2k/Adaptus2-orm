// tests/orm.test.js
const ORM = require('../index');
const { ConfigManager } = require('../src/utils/ConfigManager');

describe('Multi-Database ORM Tests', () => {
    let configManager;
    let testConfig;
    
    beforeAll(async () => {
        configManager = new ConfigManager();
        
        // Use test database configuration
        testConfig = configManager.getConfig('mysql', {
            host: process.env.TEST_MYSQL_HOST || 'localhost',
            port: process.env.TEST_MYSQL_PORT || 3306,
            user: process.env.TEST_MYSQL_USER || 'root',
            password: process.env.TEST_MYSQL_PASSWORD || 'password',
            database: process.env.TEST_MYSQL_DATABASE || 'test_orm_db'
        });
    });
    
    afterAll(async () => {
        // Clean up connections
        await ORM.cleanup();
    });
    
    describe('Connection Management', () => {
        test('should establish database connection', async () => {
            const connection = await ORM.getDbConnection(testConfig);
            expect(connection).toBeDefined();
            expect(connection.type).toBe('mysql');
        });
        
        test('should reuse existing connection', async () => {
            const connection1 = await ORM.getDbConnection(testConfig);
            const connection2 = await ORM.getDbConnection(testConfig);
            
            // Should return the same connection instance
            expect(connection1).toBe(connection2);
        });
    });
    
    describe('Schema Management', () => {
        const testTable = 'test_users';
        
        afterEach(async () => {
            // Clean up test table after each test
            try {
                await ORM.query(testConfig, `DROP TABLE IF EXISTS ${testTable}`);
            } catch (error) {
                // Ignore errors during cleanup
            }
        });
        
        test('should create table with schema', async () => {
            const result = await ORM.createTable(testConfig, testTable, {
                columns: {
                    id: { type: 'integer', primaryKey: true, autoIncrement: true },
                    name: { type: 'string', required: true },
                    email: { type: 'string', required: true, unique: true },
                    age: { type: 'integer' },
                    created_at: { type: 'timestamp', default: 'CURRENT_TIMESTAMP' }
                }
            });
            
            expect(result.success).toBe(true);
        });
        
        test('should check if table exists', async () => {
            // First create the table
            await ORM.createTable(testConfig, testTable, {
                columns: {
                    id: { type: 'integer', primaryKey: true }
                }
            });
            
            // Check if it exists
            const exists = await ORM.tableExists(testConfig, testTable);
            expect(exists.success).toBe(true);
            expect(exists.data).toBe(true);
            
            // Check non-existent table
            const notExists = await ORM.tableExists(testConfig, 'non_existent_table');
            expect(notExists.success).toBe(true);
            expect(notExists.data).toBe(false);
        });
    });
    
    describe('CRUD Operations', () => {
        const testTable = 'test_crud_table';
        
        beforeAll(async () => {
            // Create test table
            await ORM.createTable(testConfig, testTable, {
                columns: {
                    id: { type: 'integer', primaryKey: true, autoIncrement: true },
                    name: { type: 'string', required: true },
                    email: { type: 'string', required: true },
                    age: { type: 'integer' }
                }
            });
        });
        
        afterAll(async () => {
            // Clean up test table
            await ORM.query(testConfig, `DROP TABLE IF EXISTS ${testTable}`);
        });
        
        beforeEach(async () => {
            // Clear table before each test
            await ORM.query(testConfig, `TRUNCATE TABLE ${testTable}`);
        });
        
        test('should create a record', async () => {
            const result = await ORM.create(testConfig, testTable, {
                name: 'John Doe',
                email: 'john@example.com',
                age: 30
            });
            
            expect(result.success).toBe(true);
            expect(result.data.insertId).toBeDefined();
            expect(result.data.record.name).toBe('John Doe');
        });
        
        test('should read records', async () => {
            // Insert test data
            await ORM.create(testConfig, testTable, {
                name: 'John Doe',
                email: 'john@example.com',
                age: 30
            });
            
            await ORM.create(testConfig, testTable, {
                name: 'Jane Smith',
                email: 'jane@example.com',
                age: 25
            });
            
            // Read all records
            const result = await ORM.read(testConfig, testTable);
            expect(result.success).toBe(true);
            expect(result.data.length).toBe(2);
            
            // Read with conditions
            const youngUsers = await ORM.read(testConfig, testTable, {
                where: { age: 25 }
            });
            expect(youngUsers.success).toBe(true);
            expect(youngUsers.data.length).toBe(1);
            expect(youngUsers.data[0].name).toBe('Jane Smith');
        });
        
        test('should update records', async () => {
            // Insert test data
            const user = await ORM.create(testConfig, testTable, {
                name: 'John Doe',
                email: 'john@example.com',
                age: 30
            });
            
            // Update the record
            const result = await ORM.update(testConfig, testTable, {
                where: { id: user.data.insertId },
                data: { age: 31, email: 'john.doe@example.com' }
            });
            
            expect(result.success).toBe(true);
            expect(result.data.affectedRows).toBe(1);
            
            // Verify update
            const updated = await ORM.read(testConfig, testTable, {
                where: { id: user.data.insertId }
            });
            
            expect(updated.data[0].age).toBe(31);
            expect(updated.data[0].email).toBe('john.doe@example.com');
        });
        
        test('should delete records', async () => {
            // Insert test data
            const user = await ORM.create(testConfig, testTable, {
                name: 'John Doe',
                email: 'john@example.com',
                age: 30
            });
            
            // Delete the record
            const result = await ORM.deleteRecord(testConfig, testTable, {
                where: { id: user.data.insertId }
            });
            
            expect(result.success).toBe(true);
            expect(result.data.affectedRows).toBe(1);
            
            // Verify deletion
            const deleted = await ORM.read(testConfig, testTable, {
                where: { id: user.data.insertId }
            });
            
            expect(deleted.data.length).toBe(0);
        });
        
        test('should check if record exists', async () => {
            // Insert test data
            await ORM.create(testConfig, testTable, {
                name: 'John Doe',
                email: 'john@example.com',
                age: 30
            });
            
            // Check existence
            const exists = await ORM.exists(testConfig, testTable, {
                where: { email: 'john@example.com' }
            });
            
            expect(exists.success).toBe(true);
            expect(exists.data).toBe(true);
            
            // Check non-existence
            const notExists = await ORM.exists(testConfig, testTable, {
                where: { email: 'nonexistent@example.com' }
            });
            
            expect(notExists.success).toBe(true);
            expect(notExists.data).toBe(false);
        });
    });
    
    describe('Query Builder', () => {
        const { QueryBuilder } = require('../src/core/QueryBuilder');
        
        test('should build SELECT query', () => {
            const queryBuilder = QueryBuilder.create('mysql')
                .table('users')
                .select(['id', 'name', 'email'])
                .where('active', true)
                .orderBy('name', 'ASC')
                .limit(10);
            
            const { sql, params } = queryBuilder.toSQL();
            
            expect(sql).toContain('SELECT id, name, email FROM users');
            expect(sql).toContain('WHERE active = ?');
            expect(sql).toContain('ORDER BY name ASC');
            expect(sql).toContain('LIMIT 10');
            expect(params).toEqual([true]);
        });
        
        test('should build INSERT query', () => {
            const queryBuilder = QueryBuilder.create('mysql')
                .table('users')
                .insert({
                    name: 'John Doe',
                    email: 'john@example.com',
                    age: 30
                });
            
            const { sql, params } = queryBuilder.toSQL();
            
            expect(sql).toContain('INSERT INTO users');
            expect(sql).toContain('(name, email, age)');
            expect(sql).toContain('VALUES (?, ?, ?)');
            expect(params).toEqual(['John Doe', 'john@example.com', 30]);
        });
        
        test('should build UPDATE query', () => {
            const queryBuilder = QueryBuilder.create('mysql')
                .table('users')
                .update({ age: 31 })
                .where('id', 1);
            
            const { sql, params } = queryBuilder.toSQL();
            
            expect(sql).toContain('UPDATE users SET age = ?');
            expect(sql).toContain('WHERE id = ?');
            expect(params).toEqual([31, 1]);
        });
        
        test('should build DELETE query', () => {
            const queryBuilder = QueryBuilder.create('mysql')
                .table('users')
                .delete()
                .where('active', false);
            
            const { sql, params } = queryBuilder.toSQL();
            
            expect(sql).toContain('DELETE FROM users');
            expect(sql).toContain('WHERE active = ?');
            expect(params).toEqual([false]);
        });
    });
    
    describe('Error Handling', () => {
        test('should handle connection errors gracefully', async () => {
            const badConfig = configManager.getConfig('mysql', {
                host: 'nonexistent-host',
                port: 3306,
                user: 'baduser',
                password: 'badpassword',
                database: 'nonexistent'
            });
            
            try {
                await ORM.getDbConnection(badConfig);
                // Should not reach here
                expect(true).toBe(false);
            } catch (error) {
                expect(error).toBeDefined();
            }
        });
        
        test('should handle invalid queries gracefully', async () => {
            const result = await ORM.query(testConfig, 'INVALID SQL QUERY');
            
            expect(result.success).toBe(false);
            expect(result.error).toBeDefined();
        });
        
        test('should handle duplicate key errors', async () => {
            const testTable = 'test_duplicate_table';
            
            try {
                // Create table with unique constraint
                await ORM.createTable(testConfig, testTable, {
                    columns: {
                        id: { type: 'integer', primaryKey: true },
                        email: { type: 'string', unique: true }
                    }
                });
                
                // Insert first record
                await ORM.create(testConfig, testTable, {
                    id: 1,
                    email: 'duplicate@test.com'
                });
                
                // Try to insert duplicate
                const result = await ORM.create(testConfig, testTable, {
                    id: 2,
                    email: 'duplicate@test.com'
                });
                
                expect(result.success).toBe(false);
                expect(result.error).toContain('Duplicate');
                
            } finally {
                // Clean up
                await ORM.query(testConfig, `DROP TABLE IF EXISTS ${testTable}`);
            }
        });
    });
});