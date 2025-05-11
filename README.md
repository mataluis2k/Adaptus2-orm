# Multi-Database ORM for Node.js

A comprehensive, production-ready ORM that supports multiple databases including MySQL, PostgreSQL, MongoDB, and Snowflake with a unified interface.

## Features

### 🔌 Multi-Database Support
- **MySQL** - Full support with connection pooling
- **PostgreSQL** - Native async support with pg library
- **MongoDB** - Native MongoDB driver integration
- **Snowflake** - Full Snowflake cloud data warehouse support

### 🛠 Core Features
- Unified API across all databases
- Connection pooling for each database type
- Transaction support
- Query building with fluent interface
- Schema management and migrations
- Raw query execution
- Database-agnostic type mapping

### 🔧 Advanced Features
- Migration management (up/down migrations)
- Database schema introspection
- Query caching (optional)
- SSL/TLS support
- Environment-based configuration
- Comprehensive error handling
- logging capabilities

## Installation

```bash
npm install adaptus2-orm
# or
yarn add adaptus2-orm
```

### Database-specific dependencies (install as needed):
```bash
# For MySQL
npm install mysql2

# For PostgreSQL
npm install pg

# For MongoDB
npm install mongodb

# For Snowflake
npm install snowflake-sdk
```

## Quick Start

```javascript
const ORM = require('adaptus2-orm');

// Configuration
const config = {
  type: 'mysql',
  host: 'localhost',
  port: 3306,
  user: 'your_user',
  password: 'your_password',
  database: 'your_database'
};

// Initialize connection
async function main() {
  try {
    // Get database connection
    const connection = await ORM.getDbConnection(config);
    
    // Create a record
    const result = await ORM.create(config, 'users', {
      name: 'John Doe',
      email: 'john@example.com',
      age: 30
    });
    
    // Read records
    const users = await ORM.read(config, 'users', {
      where: { age: { $gte: 18 } },
      orderBy: 'name',
      limit: 10
    });
    
    // Update records
    await ORM.update(config, 'users', {
      where: { id: 1 },
      data: { age: 31 }
    });
    
    // Delete records
    await ORM.deleteRecord(config, 'users', {
      where: { id: 1 }
    });
    
  } catch (error) {
    console.error('Error:', error);
  }
}

main();
```

## Configuration

### Environment Variables

```bash
# MySQL
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=password
MYSQL_DATABASE=myapp

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
POSTGRES_DATABASE=myapp

# MongoDB
MONGODB_URI=mongodb://localhost:27017/myapp
MONGODB_HOST=localhost
MONGODB_PORT=27017
MONGODB_USER=
MONGODB_PASSWORD=
MONGODB_DATABASE=myapp

# Snowflake
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=PUBLIC
```

### Configuration Object

```javascript
const config = {
  type: 'postgresql', // mysql | postgresql | mongodb | snowflake
  host: 'localhost',
  port: 5432,
  user: 'postgres',
  password: 'password',
  database: 'myapp',
  
  // Optional pool settings
  connectionLimit: 10, // For MySQL
  max: 10, // For PostgreSQL
  maxPoolSize: 10, // For MongoDB
  
  // SSL configuration (optional)
  ssl: {
    rejectUnauthorized: false
  }
};
```

## API Reference

### Core Methods

#### `getDbConnection(config)`
Establishes and returns a database connection.

```javascript
const connection = await ORM.getDbConnection(config);
```

#### `create(config, table, data)`
Creates a new record in the specified table.

```javascript
const result = await ORM.create(config, 'users', {
  name: 'Jane Doe',
  email: 'jane@example.com'
});
```

#### `read(config, table, params)`
Reads records from the specified table.

```javascript
const users = await ORM.read(config, 'users', {
  where: { active: true },
  orderBy: 'created_at',
  order: 'DESC',
  limit: 20,
  offset: 0
});
```

#### `update(config, table, params)`
Updates records in the specified table.

```javascript
await ORM.update(config, 'users', {
  where: { id: 1 },
  data: { email: 'newemail@example.com' }
});
```

#### `deleteRecord(config, table, params)`
Deletes records from the specified table.

```javascript
await ORM.deleteRecord(config, 'users', {
  where: { active: false }
});
```

#### `query(config, queryString, params)`
Executes raw SQL/query.

```javascript
const result = await ORM.query(config, 
  'SELECT * FROM users WHERE age > ?', 
  [18]
);
```

### Query Builder

The ORM includes a powerful query builder for constructing complex queries:

```javascript
const { QueryBuilder } = require('adaptus2-orm');

const queryBuilder = QueryBuilder.create('mysql')
  .table('users')
  .select(['id', 'name', 'email'])
  .where('active', true)
  .whereIn('department', ['engineering', 'marketing'])
  .orderBy('created_at', 'DESC')
  .limit(10);

// Get the SQL and parameters
const { sql, params } = queryBuilder.toSQL();

// Execute the query
const result = await ORM.query(config, sql, params);
```

### Schema Management

```javascript
// Create a table
await ORM.createTable(config, 'products', {
  columns: {
    id: { type: 'integer', primaryKey: true, autoIncrement: true },
    name: { type: 'string', required: true },
    price: { type: 'decimal', precision: 10, scale: 2 },
    in_stock: { type: 'boolean', default: true },
    created_at: { type: 'timestamp', default: 'CURRENT_TIMESTAMP' }
  },
  indexes: {
    idx_name: { columns: ['name'], unique: true },
    idx_price: { columns: ['price'] }
  }
});

// Check if table exists
const exists = await ORM.tableExists(config, 'products');
```

### Migrations

```javascript
const { MigrationManager } = require('adaptus2-orm');

const migrationManager = new MigrationManager(ORM, config);

// Initialize migration system
await migrationManager.initialize();

// Create a new migration
await migrationManager.create('add_users_table');

// Run pending migrations
await migrationManager.migrate();

// Rollback last migration
await migrationManager.rollback();

// Get migration status
const status = await migrationManager.status();
```

### Transactions

```javascript
// For relational databases
const adapter = ORM.databaseManager.getAdapter(config.type);
const connection = await ORM.getDbConnection(config);

// Begin transaction
const txResult = await adapter.beginTransaction(connection);

try {
  // Perform operations
  await ORM.create(config, 'orders', orderData);
  await ORM.update(config, 'inventory', updateData);
  
  // Commit transaction
  await adapter.commit(connection);
} catch (error) {
  // Rollback on error
  await adapter.rollback(connection);
  throw error;
}
```

## Database-Specific Features

### MySQL
- Connection pooling with mysql2
- Prepared statements for security
- SSL/TLS support
- Automatic reconnection

### PostgreSQL
- Native prepared statements
- JSONB support
- Arrays and custom types
- Connection pooling with pg-pool

### MongoDB
- Native MongoDB driver
- Aggregation pipeline support
- GridFS for file storage
- Change streams support

### Snowflake
- Warehouse management
- Schema switching
- VARIANT type for JSON
- Bulk loading capabilities

## Examples

### Full CRUD Example

```javascript
const ORM = require('adaptus2-orm');

const config = {
  type: 'postgresql',
  host: 'localhost',
  port: 5432,
  user: 'postgres',
  password: 'password',
  database: 'myapp'
};

async function crudExample() {
  // Create
  const user = await ORM.create(config, 'users', {
    name: 'Alice Smith',
    email: 'alice@example.com',
    age: 28
  });
  
  // Read one
  const fetchedUser = await ORM.read(config, 'users', {
    where: { id: user.data.record.id }
  });
  
  // Read many with conditions
  const youngUsers = await ORM.read(config, 'users', {
    where: { age: { $lt: 30 } },
    orderBy: 'age',
    order: 'ASC'
  });
  
  // Update
  await ORM.update(config, 'users', {
    where: { id: user.data.record.id },
    data: { age: 29 }
  });
  
  // Delete
  await ORM.deleteRecord(config, 'users', {
    where: { id: user.data.record.id }
  });
}

crudExample().catch(console.error);
```

### Migration Example

```javascript
// migrations/20240501_create_users_table.js
module.exports = {
  up: async (orm, config) => {
    await orm.createTable(config, 'users', {
      columns: {
        id: { type: 'integer', primaryKey: true, autoIncrement: true },
        name: { type: 'string', required: true },
        email: { type: 'string', required: true, unique: true },
        password: { type: 'string', required: true },
        created_at: { type: 'timestamp', default: 'CURRENT_TIMESTAMP' },
        updated_at: { type: 'timestamp', default: 'CURRENT_TIMESTAMP' }
      },
      indexes: {
        idx_email: { columns: ['email'], unique: true }
      }
    });
  },

  down: async (orm, config) => {
    await orm.query(config, 'DROP TABLE IF EXISTS users');
  }
};
```

## Testing

```bash
# Run all tests
npm test

# Run tests in watch mode
npm run test:watch

# Run tests with coverage
npm run test:coverage
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by various ORM libraries like Sequelize, TypeORM, and Mongoose
- Built with love for the Node.js community
- Special thanks to all contributors

## Support

Open a discussion on Github or join the contribution and submit a PR, thanks!
---
