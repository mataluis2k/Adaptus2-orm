// src/adapters/MySQLAdapter.js
class MySQLAdapter {
    async create(connection, table, data) {
        try {
            const fields = Object.keys(data);
            const values = Object.values(data);
            const placeholders = fields.map(() => '?').join(',');
            
            const query = `INSERT INTO ${table} (${fields.join(',')}) VALUES (${placeholders})`;
            
            const [result] = await connection.pool.execute(query, values);
            
            return {
                success: true,
                data: {
                    insertId: result.insertId,
                    affectedRows: result.affectedRows,
                    record: { ...data, id: result.insertId }
                }
            };
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
            
            if (params.where) {
                const conditions = Object.keys(params.where).map(key => `${key} = ?`).join(' AND ');
                query += ` WHERE ${conditions}`;
                queryParams = Object.values(params.where);
            }
            
            if (params.orderBy) {
                query += ` ORDER BY ${params.orderBy} ${params.order || 'ASC'}`;
            }
            
            if (params.limit) {
                query += ` LIMIT ${parseInt(params.limit)}`;
                if (params.offset) {
                    query += ` OFFSET ${parseInt(params.offset)}`;
                }
            }
            
            const [rows] = await connection.pool.execute(query, queryParams);
            
            return {
                success: true,
                data: rows
            };
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
            
            const setFields = Object.keys(data).map(key => `${key} = ?`).join(',');
            const setValues = Object.values(data);
            
            const whereFields = Object.keys(where).map(key => `${key} = ?`).join(' AND ');
            const whereValues = Object.values(where);
            
            const query = `UPDATE ${table} SET ${setFields} WHERE ${whereFields}`;
            const queryParams = [...setValues, ...whereValues];
            
            const [result] = await connection.pool.execute(query, queryParams);
            
            return {
                success: true,
                data: {
                    affectedRows: result.affectedRows,
                    changedRows: result.changedRows
                }
            };
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
            
            const whereFields = Object.keys(params.where).map(key => `${key} = ?`).join(' AND ');
            const whereValues = Object.values(params.where);
            
            const query = `DELETE FROM ${table} WHERE ${whereFields}`;
            
            const [result] = await connection.pool.execute(query, whereValues);
            
            return {
                success: true,
                data: {
                    affectedRows: result.affectedRows
                }
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async exists(connection, table, params) {
        try {
            let query = `SELECT COUNT(*) as count FROM ${table}`;
            let queryParams = [];
            
            if (params.where) {
                const conditions = Object.keys(params.where).map(key => `${key} = ?`).join(' AND ');
                query += ` WHERE ${conditions}`;
                queryParams = Object.values(params.where);
            }
            
            const [rows] = await connection.pool.execute(query, queryParams);
            
            return {
                success: true,
                data: rows[0].count > 0
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async query(connection, queryString, params = []) {
        try {
            // Determine if query is SELECT or modification query
            const isSelectQuery = queryString.trim().toUpperCase().startsWith('SELECT');
            
            let result;
            if (isSelectQuery) {
                [result] = await connection.pool.execute(queryString, params);
            } else {
                [result] = await connection.pool.execute(queryString, params);
            }
            
            return {
                success: true,
                data: result
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async initDatabase(connection, config) {
        try {
            // Test the connection
            await connection.pool.query('SELECT 1');
            
            return {
                success: true,
                data: 'Database initialized successfully'
            };
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
            const placeholders = fields.map(() => '?').join(',');
            const valuePlaceholders = records.map(() => `(${placeholders})`).join(',');
            
            const query = `INSERT INTO ${table} (${fields.join(',')}) VALUES ${valuePlaceholders}`;
            
            const allValues = [];
            records.forEach(record => {
                allValues.push(...Object.values(record));
            });
            
            const [result] = await connection.pool.execute(query, allValues);
            
            return {
                success: true,
                data: {
                    affectedRows: result.affectedRows,
                    insertId: result.insertId
                }
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async upsert(connection, table, data, updateOnConflict = null) {
        try {
            const fields = Object.keys(data);
            const values = Object.values(data);
            const placeholders = fields.map(() => '?').join(',');
            
            let query = `INSERT INTO ${table} (${fields.join(',')}) VALUES (${placeholders})`;
            
            if (updateOnConflict) {
                const updateFields = updateOnConflict.map(field => `${field} = VALUES(${field})`).join(',');
                query += ` ON DUPLICATE KEY UPDATE ${updateFields}`;
            }
            
            const [result] = await connection.pool.execute(query, values);
            
            return {
                success: true,
                data: {
                    insertId: result.insertId,
                    affectedRows: result.affectedRows
                }
            };
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
            await connection.pool.query('START TRANSACTION');
            return { success: true };
        } catch (error) {
            return { success: false, error: error.message };
        }
    }

    async commit(connection) {
        try {
            await connection.pool.query('COMMIT');
            return { success: true };
        } catch (error) {
            return { success: false, error: error.message };
        }
    }

    async rollback(connection) {
        try {
            await connection.pool.query('ROLLBACK');
            return { success: true };
        } catch (error) {
            return { success: false, error: error.message };
        }
    }
}

module.exports = MySQLAdapter;