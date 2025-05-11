const migrationManager = new MigrationManager(ORM, config);

// Initialize migration system
await migrationManager.initialize();

// Create a new migration
await migrationManager.create('add_users_table');

// Run pending migrations
await migrationManager.migrate();

// Check migration status
const status = await migrationManager.status();

// Rollback last batch
await migrationManager.rollback();

// Generate migration from schema differences
await migrationManager.generateFromSchema(oldSchema, newSchema, 'update_schema');

// Dry run pending migrations
const dryRunResult = await migrationManager.dryRun();