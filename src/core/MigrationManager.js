// src/core/MigrationManager.js
const path = require('path');
const fs = require('fs').promises;

class MigrationManager {
    constructor(orm, config) {
        this.orm = orm;
        this.config = config;
        this.migrationsPath = config.migrationsPath || './migrations';
        this.migrationTable = config.migrationTable || 'migrations';
    }

    // Initialize migration system
    async initialize() {
        try {
            // Create migrations directory if it doesn't exist
            await fs.mkdir(this.migrationsPath, { recursive: true });
            
            // Create migration tracking table
            await this._createMigrationTable();
            
            return { success: true };
        } catch (error) {
            return { success: false, error: error.message };
        }
    }

    // Create migration tracking table
    async _createMigrationTable() {
        const { type } = this.config;
        
        const schema = {
            columns: {
                id: { type: 'integer', primaryKey: true, autoIncrement: true },
                migration: { type: 'string', required: true, unique: true },
                batch: { type: 'integer', required: true },
                executed_at: { type: 'timestamp', required: true, default: 'CURRENT_TIMESTAMP' }
            }
        };
        
        // Check if table exists first
        const exists = await this.orm.tableExists(this.config, this.migrationTable);
        
        if (!exists.data) {
            await this.orm.createTable(this.config, this.migrationTable, schema);
        }
    }

    // Create a new migration file
    async create(name) {
        try {
            const timestamp = new Date().toISOString().replace(/[^0-9]/g, '').slice(0, 14);
            const fileName = `${timestamp}_${name.toLowerCase().replace(/\s+/g, '_')}.js`;
            const filePath = path.join(this.migrationsPath, fileName);
            
            const template = this._getMigrationTemplate(name);
            
            await fs.writeFile(filePath, template);
            
            return { 
                success: true, 
                data: { 
                    fileName,
                    filePath 
                } 
            };
        } catch (error) {
            return { 
                success: false, 
                error: error.message 
            };
        }
    }

    // Get migration file template
    _getMigrationTemplate(name) {
        return `// Migration: ${name}
// Created at: ${new Date().toISOString()}

module.exports = {
    up: async (orm, config) => {
        // Add your migration logic here
        // Example:
        // await orm.createTable(config, 'new_table', {
        //     columns: {
        //         id: { type: 'integer', primaryKey: true, autoIncrement: true },
        //         name: { type: 'string', required: true },
        //         created_at: { type: 'timestamp', required: true, default: 'CURRENT_TIMESTAMP' }
        //     }
        // });
    },

    down: async (orm, config) => {
        // Add your rollback logic here
        // Example:
        // await orm.query(config, 'DROP TABLE IF EXISTS new_table');
    }
};
`;
    }

    // Run pending migrations
    async migrate() {
        try {
            const pendingMigrations = await this._getPendingMigrations();
            
            if (pendingMigrations.length === 0) {
                return { 
                    success: true, 
                    data: { 
                        message: 'No pending migrations',
                        executed: []
                    }
                };
            }
            
            const currentBatch = await this._getNextBatchNumber();
            const executed = [];
            const errors = [];
            
            for (const migration of pendingMigrations) {
                try {
                    await this._executeMigration(migration, currentBatch);
                    executed.push(migration);
                } catch (error) {
                    console.error(`Failed to execute migration ${migration}:`, error);
                    errors.push({ migration, error: error.message });
                    break; // Stop on first error
                }
            }
            
            return { 
                success: errors.length === 0, 
                data: { 
                    message: `Executed ${executed.length} migrations`,
                    executed,
                    batch: currentBatch,
                    errors
                }
            };
        } catch (error) {
            return { 
                success: false, 
                error: error.message,
                data: { executed: [] }
            };
        }
    }

    // Get pending migrations
    async _getPendingMigrations() {
        // Get all migration files
        const files = await fs.readdir(this.migrationsPath);
        const migrationFiles = files
            .filter(file => file.endsWith('.js'))
            .sort();
        
        // Get executed migrations from database
        const executedResult = await this.orm.read(this.config, this.migrationTable, {
            orderBy: 'migration',
            order: 'ASC'
        });
        
        const executedMigrations = executedResult.success 
            ? executedResult.data.map(row => row.migration)
            : [];
        
        // Find pending migrations
        return migrationFiles.filter(file => !executedMigrations.includes(file));
    }

    // Execute a single migration
    async _executeMigration(filename, batch) {
        const migrationPath = path.join(this.migrationsPath, filename);
        delete require.cache[require.resolve(path.resolve(migrationPath))]; // Clear cache
        const migration = require(path.resolve(migrationPath));
        
        if (typeof migration.up !== 'function') {
            throw new Error(`Migration ${filename} must export an 'up' function`);
        }
        
        // Execute the migration
        await migration.up(this.orm, this.config);
        
        // Record migration in database
        await this.orm.create(this.config, this.migrationTable, {
            migration: filename,
            batch: batch,
            executed_at: new Date()
        });
    }

    // Get the next batch number
    async _getNextBatchNumber() {
        const result = await this.orm.query(
            this.config,
            `SELECT MAX(batch) as max_batch FROM ${this.migrationTable}`
        );
        
        if (result.success && result.data.length > 0) {
            const maxBatch = result.data[0].max_batch || result.data[0].MAX_BATCH || 0;
            return maxBatch + 1;
        }
        
        return 1;
    }

    // Rollback migrations
    async rollback(batch = null) {
        try {
            let migrationsToRollback;
            
            if (batch !== null) {
                // Rollback specific batch
                const result = await this.orm.read(this.config, this.migrationTable, {
                    where: { batch: batch },
                    orderBy: 'migration',
                    order: 'DESC'
                });
                
                migrationsToRollback = result.success ? result.data : [];
            } else {
                // Rollback last batch
                const lastBatch = await this._getLastBatchNumber();
                
                if (lastBatch === 0) {
                    return { 
                        success: true, 
                        data: { 
                            message: 'No migrations to rollback',
                            rolledBack: []
                        }
                    };
                }
                
                const result = await this.orm.read(this.config, this.migrationTable, {
                    where: { batch: lastBatch },
                    orderBy: 'migration',
                    order: 'DESC'
                });
                
                migrationsToRollback = result.success ? result.data : [];
            }
            
            const rolledBack = [];
            const errors = [];
            
            for (const migrationRecord of migrationsToRollback) {
                try {
                    await this._rollbackMigration(migrationRecord.migration);
                    rolledBack.push(migrationRecord.migration);
                } catch (error) {
                    console.error(`Failed to rollback migration ${migrationRecord.migration}:`, error);
                    errors.push({ migration: migrationRecord.migration, error: error.message });
                    break; // Stop on first error
                }
            }
            
            return { 
                success: errors.length === 0, 
                data: { 
                    message: `Rolled back ${rolledBack.length} migrations`,
                    rolledBack,
                    errors
                }
            };
        } catch (error) {
            return { 
                success: false, 
                error: error.message,
                data: { rolledBack: [] }
            };
        }
    }

    // Rollback a single migration
    async _rollbackMigration(filename) {
        const migrationPath = path.join(this.migrationsPath, filename);
        delete require.cache[require.resolve(path.resolve(migrationPath))]; // Clear cache
        const migration = require(path.resolve(migrationPath));
        
        if (typeof migration.down !== 'function') {
            throw new Error(`Migration ${filename} must export a 'down' function`);
        }
        
        // Execute the rollback
        await migration.down(this.orm, this.config);
        
        // Remove migration record from database
        await this.orm.deleteRecord(this.config, this.migrationTable, {
            where: { migration: filename }
        });
    }

    // Get the last batch number
    async _getLastBatchNumber() {
        const result = await this.orm.query(
            this.config,
            `SELECT MAX(batch) as max_batch FROM ${this.migrationTable}`
        );
        
        if (result.success && result.data.length > 0) {
            return result.data[0].max_batch || result.data[0].MAX_BATCH || 0;
        }
        
        return 0;
    }

    // Get migration status
    async status() {
        try {
            // Get all migration files
            const files = await fs.readdir(this.migrationsPath);
            const migrationFiles = files
                .filter(file => file.endsWith('.js'))
                .sort();
            
            // Get executed migrations from database
            const executedResult = await this.orm.read(this.config, this.migrationTable, {
                orderBy: 'migration',
                order: 'ASC'
            });
            
            const executedMigrations = executedResult.success 
                ? executedResult.data.reduce((acc, row) => {
                    acc[row.migration] = {
                        batch: row.batch,
                        executed_at: row.executed_at
                    };
                    return acc;
                }, {})
                : {};
            
            // Build status for each migration
            const status = migrationFiles.map(file => ({
                migration: file,
                status: executedMigrations[file] ? 'executed' : 'pending',
                batch: executedMigrations[file]?.batch || null,
                executed_at: executedMigrations[file]?.executed_at || null
            }));
            
            return { 
                success: true, 
                data: {
                    migrations: status,
                    summary: {
                        total: migrationFiles.length,
                        executed: Object.keys(executedMigrations).length,
                        pending: migrationFiles.length - Object.keys(executedMigrations).length
                    }
                }
            };
        } catch (error) {
            return { 
                success: false, 
                error: error.message 
            };
        }
    }

    // Reset all migrations (DANGEROUS!)
    async reset() {
        try {
            // Get all executed migrations in reverse order
            const executedResult = await this.orm.read(this.config, this.migrationTable, {
                orderBy: 'batch DESC, migration',
                order: 'DESC'
            });
            
            if (!executedResult.success || !executedResult.data.length) {
                return { 
                    success: true, 
                    data: { 
                        message: 'No migrations to reset',
                        resetMigrations: []
                    }
                };
            }
            
            const resetMigrations = [];
            const errors = [];
            
            for (const migrationRecord of executedResult.data) {
                try {
                    await this._rollbackMigration(migrationRecord.migration);
                    resetMigrations.push(migrationRecord.migration);
                } catch (error) {
                    console.error(`Failed to reset migration ${migrationRecord.migration}:`, error);
                    errors.push({ migration: migrationRecord.migration, error: error.message });
                    // Continue with other migrations even if one fails
                }
            }
            
            return { 
                success: errors.length === 0, 
                data: { 
                    message: `Reset ${resetMigrations.length} migrations`,
                    resetMigrations,
                    errors
                }
            };
        } catch (error) {
            return { 
                success: false, 
                error: error.message,
                data: { resetMigrations: [] }
            };
        }
    }

    // Refresh migrations (rollback all and migrate again)
    async refresh() {
        try {
            // Reset all migrations
            const resetResult = await this.reset();
            
            if (!resetResult.success) {
                return resetResult;
            }
            
            // Run all migrations again
            const migrateResult = await this.migrate();
            
            if (!migrateResult.success) {
                return migrateResult;
            }
            
            return { 
                success: true, 
                data: { 
                    message: 'Database refreshed successfully',
                    rolledBack: resetResult.data.resetMigrations,
                    migrated: migrateResult.data.executed
                }
            };
        } catch (error) {
            return { 
                success: false, 
                error: error.message 
            };
        }
    }

    // Generate migration from schema differences
    async generateFromSchema(fromSchema, toSchema, migrationName) {
        try {
            // Compare schemas and generate migration
            const differences = this._compareSchemas(fromSchema, toSchema);
            const migrationCode = this._generateMigrationCode(differences, migrationName);
            
            // Create migration file
            const timestamp = new Date().toISOString().replace(/[^0-9]/g, '').slice(0, 14);
            const fileName = `${timestamp}_${migrationName.toLowerCase().replace(/\s+/g, '_')}.js`;
            const filePath = path.join(this.migrationsPath, fileName);
            
            await fs.writeFile(filePath, migrationCode);
            
            return { 
                success: true, 
                data: { 
                    fileName,
                    filePath,
                    differences
                }
            };
        } catch (error) {
            return { 
                success: false, 
                error: error.message 
            };
        }
    }

    // Compare two schemas and return differences
    _compareSchemas(fromSchema, toSchema) {
        const differences = {
            addedTables: [],
            droppedTables: [],
            modifiedTables: {},
            addedColumns: {},
            droppedColumns: {},
            modifiedColumns: {},
            addedIndexes: {},
            droppedIndexes: {}
        };
        
        // Find added and dropped tables
        const fromTables = Object.keys(fromSchema || {});
        const toTables = Object.keys(toSchema || {});
        
        differences.addedTables = toTables.filter(table => !fromTables.includes(table));
        differences.droppedTables = fromTables.filter(table => !toTables.includes(table));
        
        // Compare existing tables
        const commonTables = fromTables.filter(table => toTables.includes(table));
        
        for (const table of commonTables) {
            const fromTable = fromSchema[table];
            const toTable = toSchema[table];
            
            // Compare columns
            const fromColumns = Object.keys(fromTable.columns || {});
            const toColumns = Object.keys(toTable.columns || {});
            
            const addedColumns = toColumns.filter(col => !fromColumns.includes(col));
            const droppedColumns = fromColumns.filter(col => !toColumns.includes(col));
            
            if (addedColumns.length > 0) {
                differences.addedColumns[table] = addedColumns.map(col => ({
                    name: col,
                    definition: toTable.columns[col]
                }));
            }
            
            if (droppedColumns.length > 0) {
                differences.droppedColumns[table] = droppedColumns.map(col => ({
                    name: col,
                    definition: fromTable.columns[col]
                }));
            }
            
            // Compare modified columns
            const commonColumns = fromColumns.filter(col => toColumns.includes(col));
            const modifiedColumns = [];
            
            for (const col of commonColumns) {
                if (JSON.stringify(fromTable.columns[col]) !== JSON.stringify(toTable.columns[col])) {
                    modifiedColumns.push({
                        name: col,
                        from: fromTable.columns[col],
                        to: toTable.columns[col]
                    });
                }
            }
            
            if (modifiedColumns.length > 0) {
                differences.modifiedColumns[table] = modifiedColumns;
            }
            
            // Compare indexes
            const fromIndexes = Object.keys(fromTable.indexes || {});
            const toIndexes = Object.keys(toTable.indexes || {});
            
            const addedIndexes = toIndexes.filter(idx => !fromIndexes.includes(idx));
            const droppedIndexes = fromIndexes.filter(idx => !toIndexes.includes(idx));
            
            if (addedIndexes.length > 0) {
                differences.addedIndexes[table] = addedIndexes.map(idx => ({
                    name: idx,
                    definition: toTable.indexes[idx]
                }));
            }
            
            if (droppedIndexes.length > 0) {
                differences.droppedIndexes[table] = droppedIndexes.map(idx => ({
                    name: idx,
                    definition: fromTable.indexes[idx]
                }));
            }
        }
        
        return differences;
    }

    // Generate migration code from differences
    _generateMigrationCode(differences, migrationName) {
        let upCode = '';
        let downCode = '';
        
        // Generate code for added tables
        differences.addedTables.forEach(table => {
            upCode += `
        // Create table ${table}
        await orm.createTable(config, '${table}', {
            // Add table definition here
        });`;
            
            downCode += `
        // Drop table ${table}
        await orm.query(config, 'DROP TABLE IF EXISTS ${table}');`;
        });
        
        // Generate code for dropped tables
        differences.droppedTables.forEach(table => {
            upCode += `
        // Drop table ${table}
        await orm.query(config, 'DROP TABLE IF EXISTS ${table}');`;
            
            downCode += `
        // Recreate table ${table}
        await orm.createTable(config, '${table}', {
            // Add table definition here
        });`;
        });
        
        // Generate code for added columns
        Object.entries(differences.addedColumns).forEach(([table, columns]) => {
            columns.forEach(column => {
                upCode += `
        // Add column ${column.name} to ${table}
        await orm.schemaManager.addColumn(connection, config.type, '${table}', '${column.name}', ${JSON.stringify(column.definition)});`;
                
                downCode += `
        // Drop column ${column.name} from ${table}
        await orm.schemaManager.dropColumn(connection, config.type, '${table}', '${column.name}');`;
            });
        });
        
        // Generate code for dropped columns
        Object.entries(differences.droppedColumns).forEach(([table, columns]) => {
            columns.forEach(column => {
                upCode += `
        // Drop column ${column.name} from ${table}
        await orm.schemaManager.dropColumn(connection, config.type, '${table}', '${column.name}');`;
                
                downCode += `
        // Add column ${column.name} to ${table}
        await orm.schemaManager.addColumn(connection, config.type, '${table}', '${column.name}', ${JSON.stringify(column.definition)});`;
            });
        });
        
        // Generate code for modified columns
        Object.entries(differences.modifiedColumns).forEach(([table, columns]) => {
            columns.forEach(column => {
                upCode += `
        // Modify column ${column.name} in ${table}
        await orm.query(config, \`ALTER TABLE ${table} MODIFY COLUMN ${column.name} ${this._getColumnDefinition(column.to)}\`);`;
                
                downCode += `
        // Revert column ${column.name} in ${table}
        await orm.query(config, \`ALTER TABLE ${table} MODIFY COLUMN ${column.name} ${this._getColumnDefinition(column.from)}\`);`;
            });
        });
        
        // Generate code for added indexes
        Object.entries(differences.addedIndexes).forEach(([table, indexes]) => {
            indexes.forEach(index => {
                upCode += `
        // Add index ${index.name} to ${table}
        await orm.schemaManager.createIndex(connection, config.type, '${table}', '${index.name}', ${JSON.stringify(index.definition)});`;
                
                downCode += `
        // Drop index ${index.name}
        await orm.query(config, 'DROP INDEX ${index.name}');`;
            });
        });
        
        // Generate code for dropped indexes
        Object.entries(differences.droppedIndexes).forEach(([table, indexes]) => {
            indexes.forEach(index => {
                upCode += `
        // Drop index ${index.name}
        await orm.query(config, 'DROP INDEX ${index.name}');`;
                
                downCode += `
        // Recreate index ${index.name} on ${table}
        await orm.schemaManager.createIndex(connection, config.type, '${table}', '${index.name}', ${JSON.stringify(index.definition)});`;
            });
        });
        
        return `// Migration: ${migrationName}
// Created at: ${new Date().toISOString()}
// Auto-generated from schema comparison

module.exports = {
    up: async (orm, config) => {
        const connection = await orm.getDbConnection(config);${upCode || '\n        // No changes needed'}
    },

    down: async (orm, config) => {
        const connection = await orm.getDbConnection(config);${downCode || '\n        // No rollback needed'}
    }
};
`;
    }

    // Helper method to generate column definition
    _getColumnDefinition(columnDef) {
        const type = columnDef.type.toUpperCase();
        let definition = type;
        
        if (columnDef.length) {
            definition += `(${columnDef.length})`;
        }
        
        if (columnDef.precision && columnDef.scale) {
            definition += `(${columnDef.precision},${columnDef.scale})`;
        }
        
        if (columnDef.required) {
            definition += ' NOT NULL';
        }
        
        if (columnDef.default !== undefined) {
            definition += ` DEFAULT ${columnDef.default}`;
        }
        
        return definition;
    }

    // Validate migration file
    async validateMigration(migrationPath) {
        try {
            delete require.cache[require.resolve(path.resolve(migrationPath))];
            const migration = require(path.resolve(migrationPath));
            
            const errors = [];
            
            if (typeof migration.up !== 'function') {
                errors.push('Migration must export an "up" function');
            }
            
            if (typeof migration.down !== 'function') {
                errors.push('Migration must export a "down" function');
            }
            
            return {
                success: errors.length === 0,
                errors: errors
            };
        } catch (error) {
            return {
                success: false,
                errors: [error.message]
            };
        }
    }

    // Get migration history
    async getHistory(options = {}) {
        try {
            const params = {
                orderBy: options.orderBy || 'executed_at',
                order: options.order || 'DESC'
            };
            
            if (options.limit) {
                params.limit = options.limit;
            }
            
            if (options.batch) {
                params.where = { batch: options.batch };
            }
            
            const result = await this.orm.read(this.config, this.migrationTable, params);
            
            return {
                success: result.success,
                data: result.data,
                error: result.error
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    // Dry run migration (validate without executing)
    async dryRun() {
        try {
            const pendingMigrations = await this._getPendingMigrations();
            const validations = [];
            
            for (const migration of pendingMigrations) {
                const migrationPath = path.join(this.migrationsPath, migration);
                const validation = await this.validateMigration(migrationPath);
                
                validations.push({
                    migration,
                    valid: validation.success,
                    errors: validation.errors
                });
            }
            
            return {
                success: true,
                data: {
                    pendingMigrations,
                    validations,
                    allValid: validations.every(v => v.valid)
                }
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }
}

module.exports = MigrationManager;