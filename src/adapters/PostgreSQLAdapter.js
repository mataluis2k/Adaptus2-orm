// src/adapters/PostgreSQLAdapter.js
class PostgreSQLAdapter {
    async create(connection, table, data) {
        try {
            const fields = Object.keys(data);
            const values = Object.values(data);
            const placeholders = fields.map((_, index) => `$${index + 1}`).join(',');
            
            const query = `INSERT INTO ${table} (${fields.join(',')}) VALUES (${placeholders}) RETURNING *`;
            
            const client = await connection.pool.connect();
            try {
                const result = await client.query(query, values);
                return {
                    success: true,
                    data: {
                        record: result.rows[0],
                        affectedRows: result.rowCount
                    }
                };
            } finally {
                client.release();
            }
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async read(connection, table, params = {}) {
        try {
            let query = `SELECT * FROM ${table}`;
            let queryParams = [];
            let paramCounter = 1;
            
            if (params.where) {
                const conditions = Object.keys(params.where).map(key => {
                    queryParams.push(params.where[key]);
                    return `${key} = $${paramCounter++}`;
                }).join(' AND ');
                query += ` WHERE ${conditions}`;
            }
            
            if (params.orderBy) {
                query += ` ORDER BY ${params.orderBy} ${params.order || 'ASC'}`;
            }
            
            if (params.limit) {
                query += ` LIMIT $${paramCounter++}`;
                queryParams.push(parseInt(params.limit));
                
                if (params.offset) {
                    query += ` OFFSET $${paramCounter++}`;
                    queryParams.push(parseInt(params.offset));
                }
            }
            
            const client = await connection.pool.connect();
            try {
                const result = await client.query(query, queryParams);
                return {
                    success: true,
                    data: result.rows
                };
            } finally {
                client.release();
            }
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async update(connection, table, params) {
        try {
            const { where, data } = params;
            
            if (!where || !data) {
                throw new Error('Both where and data parameters are required for update');
            }
            
            let paramCounter = 1;
            const setFields = Object.keys(data).map(key => {
                return `${key} = $${paramCounter++}`;
            }).join(',');
            const setValues = Object.values(data);
            
            const whereFields = Object.keys(where).map(key => {
                return `${key} = $${paramCounter++}`;
            }).join(' AND ');
            const whereValues = Object.values(where);
            
            const query = `UPDATE ${table} SET ${setFields} WHERE ${whereFields} RETURNING *`;
            const queryParams = [...setValues, ...whereValues];
            
            const client = await connection.pool.connect();
            try {
                const result = await client.query(query, queryParams);
                return {
                    success: true,
                    data: {
                        records: result.rows,
                        affectedRows: result.rowCount
                    }
                };
            } finally {
                client.release();
            }
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async delete(connection, table, params) {
        try {
            if (!params.where) {
                throw new Error('Where parameter is required for delete');
            }
            
            let paramCounter = 1;
            const whereFields = Object.keys(params.where).map(key => {
                return `${key} = $${paramCounter++}`;
            }).join(' AND ');
            const whereValues = Object.values(params.where);
            
            const query = `DELETE FROM ${table} WHERE ${whereFields} RETURNING *`;
            
            const client = await connection.pool.connect();
            try {
                const result = await client.query(query, whereValues);
                return {
                    success: true,
                    data: {
                        deletedRecords: result.rows,
                        affectedRows: result.rowCount
                    }
                };
            } finally {
                client.release();
            }
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async exists(connection, table, params) {
        try {
            let query = `SELECT EXISTS(SELECT 1 FROM ${table}`;
            let queryParams = [];
            let paramCounter = 1;
            
            if (params.where) {
                const conditions = Object.keys(params.where).map(key => {
                    queryParams.push(params.where[key]);
                    return `${key} = $${paramCounter++}`;
                }).join(' AND ');
                query += ` WHERE ${conditions}`;
            }
            
            query += ')';
            
            const client = await connection.pool.connect();
            try {
                const result = await client.query(query, queryParams);
                return {
                    success: true,
                    data: result.rows[0].exists
                };
            } finally {
                client.release();
            }
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async query(connection, queryString, params = []) {
        try {
            const client = await connection.pool.connect();
            try {
                const result = await client.query(queryString, params);
                return {
                    success: true,
                    data: result.rows || result
                };
            } finally {
                client.release();
            }
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async initDatabase(connection, config) {
        try {
            const client = await connection.pool.connect();
            try {
                await client.query('SELECT 1');
                return {
                    success: true,
                    data: 'Database initialized successfully'
                };
            } finally {
                client.release();
            }
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    // Utility methods for batch operations
    async bulkInsert(connection, table, records) {
        try {
            if (!records.length) {
                return { success: true, data: { affectedRows: 0 } };
            }
            
            const fields = Object.keys(records[0]);
            const placeholders = records.map((_, recordIndex) => {
                return `(${fields.map((_, fieldIndex) => 
                    `$${recordIndex * fields.length + fieldIndex + 1}`
                ).join(',')})`;
            }).join(',');
            
            const query = `INSERT INTO ${table} (${fields.join(',')}) VALUES ${placeholders} RETURNING *`;
            
            const allValues = [];
            records.forEach(record => {
                allValues.push(...Object.values(record));
            });
            
            const client = await connection.pool.connect();
            try {
                const result = await client.query(query, allValues);
                return {
                    success: true,
                    data: {
                        records: result.rows,
                        affectedRows: result.rowCount
                    }
                };
            } finally {
                client.release();
            }
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async upsert(connection, table, data, conflictFields = [], updateFields = []) {
        try {
            const fields = Object.keys(data);
            const values = Object.values(data);
            const placeholders = fields.map((_, index) => `$${index + 1}`).join(',');
            
            let query = `INSERT INTO ${table} (${fields.join(',')}) VALUES (${placeholders})`;
            
            if (conflictFields.length > 0) {
                query += ` ON CONFLICT (${conflictFields.join(',')}) DO`;
                
                if (updateFields.length > 0) {
                    const setClause = updateFields.map(field => 
                        `${field} = EXCLUDED.${field}`
                    ).join(',');
                    query += ` UPDATE SET ${setClause}`;
                } else {
                    query += ' NOTHING';
                }
            }
            
            query += ' RETURNING *';
            
            const client = await connection.pool.connect();
            try {
                const result = await client.query(query, values);
                return {
                    success: true,
                    data: {
                        record: result.rows[0],
                        affectedRows: result.rowCount
                    }
                };
            } finally {
                client.release();
            }
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    // Transaction support
    async beginTransaction(connection) {
        try {
            const client = await connection.pool.connect();
            await client.query('BEGIN');
            return { success: true, client };
        } catch (error) {
            return { success: false, error: error.message };
        }
    }

    async commit(client) {
        try {
            await client.query('COMMIT');
            client.release();
            return { success: true };
        } catch (error) {
            return { success: false, error: error.message };
        }
    }

    async rollback(client) {
        try {
            await client.query('ROLLBACK');
            client.release();
            return { success: true };
        } catch (error) {
            return { success: false, error: error.message };
        }
    }

    // Advanced PostgreSQL features
    async createEnum(connection, enumName, values) {
        try {
            const query = `CREATE TYPE ${enumName} AS ENUM (${values.map(v => `'${v}'`).join(',')})`;
            
            const client = await connection.pool.connect();
            try {
                await client.query(query);
                return {
                    success: true,
                    data: `Enum ${enumName} created successfully`
                };
            } finally {
                client.release();
            }
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async dropEnum(connection, enumName) {
        try {
            const query = `DROP TYPE IF EXISTS ${enumName}`;
            
            const client = await connection.pool.connect();
            try {
                await client.query(query);
                return {
                    success: true,
                    data: `Enum ${enumName} dropped successfully`
                };
            } finally {
                client.release();
            }
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    // JSONB operations
    async jsonbQuery(connection, table, column, jsonPath, params = {}) {
        try {
            let query = `SELECT * FROM ${table} WHERE ${column} @> $1`;
            let queryParams = [jsonPath];
            let paramCounter = 2;
            
            if (params.where) {
                const conditions = Object.keys(params.where).map(key => {
                    queryParams.push(params.where[key]);
                    return `${key} = $${paramCounter++}`;
                }).join(' AND ');
                query += ` AND ${conditions}`;
            }
            
            const client = await connection.pool.connect();
            try {
                const result = await client.query(query, queryParams);
                return {
                    success: true,
                    data: result.rows
                };
            } finally {
                client.release();
            }
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    // Array operations
    async arrayContains(connection, table, column, value, params = {}) {
        try {
            let query = `SELECT * FROM ${table} WHERE $1 = ANY(${column})`;
            let queryParams = [value];
            let paramCounter = 2;
            
            if (params.where) {
                const conditions = Object.keys(params.where).map(key => {
                    queryParams.push(params.where[key]);
                    return `${key} = $${paramCounter++}`;
                }).join(' AND ');
                query += ` AND ${conditions}`;
            }
            
            const client = await connection.pool.connect();
            try {
                const result = await client.query(query, queryParams);
                return {
                    success: true,
                    data: result.rows
                };
            } finally {
                client.release();
            }
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    // Full-text search
    async fullTextSearch(connection, table, column, searchText, params = {}) {
        try {
            let query = `SELECT * FROM ${table} WHERE to_tsvector('english', ${column}) @@ to_tsquery('english', $1)`;
            let queryParams = [searchText];
            let paramCounter = 2;
            
            if (params.where) {
                const conditions = Object.keys(params.where).map(key => {
                    queryParams.push(params.where[key]);
                    return `${key} = $${paramCounter++}`;
                }).join(' AND ');
                query += ` AND ${conditions}`;
            }
            
            // Add ranking if requested
            if (params.ranked) {
                query = query.replace('SELECT *', 'SELECT *, ts_rank_cd(to_tsvector(\'english\', ' + column + '), to_tsquery(\'english\', $1)) AS rank');
                query += ' ORDER BY rank DESC';
            }
            
            const client = await connection.pool.connect();
            try {
                const result = await client.query(query, queryParams);
                return {
                    success: true,
                    data: result.rows
                };
            } finally {
                client.release();
            }
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    // Create extension
    async createExtension(connection, extensionName) {
        try {
            const query = `CREATE EXTENSION IF NOT EXISTS ${extensionName}`;
            
            const client = await connection.pool.connect();
            try {
                await client.query(query);
                return {
                    success: true,
                    data: `Extension ${extensionName} created successfully`
                };
            } finally {
                client.release();
            }
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    // Vacuum and analyze
    async vacuum(connection, table, options = {}) {
        try {
            let query = 'VACUUM';
            
            if (options.full) query += ' FULL';
            if (options.analyze) query += ' ANALYZE';
            if (table) query += ` ${table}`;
            
            const client = await connection.pool.connect();
            try {
                await client.query(query);
                return {
                    success: true,
                    data: 'Vacuum completed successfully'
                };
            } finally {
                client.release();
            }
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    // Create materialized view
    async createMaterializedView(connection, viewName, query) {
        try {
            const createQuery = `CREATE MATERIALIZED VIEW ${viewName} AS ${query}`;
            
            const client = await connection.pool.connect();
            try {
                await client.query(createQuery);
                return {
                    success: true,
                    data: `Materialized view ${viewName} created successfully`
                };
            } finally {
                client.release();
            }
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    // Refresh materialized view
    async refreshMaterializedView(connection, viewName, concurrently = false) {
        try {
            const query = `REFRESH MATERIALIZED VIEW ${concurrently ? 'CONCURRENTLY' : ''} ${viewName}`;
            
            const client = await connection.pool.connect();
            try {
                await client.query(query);
                return {
                    success: true,
                    data: `Materialized view ${viewName} refreshed successfully`
                };
            } finally {
                client.release();
            }
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }
}

module.exports = PostgreSQLAdapter;