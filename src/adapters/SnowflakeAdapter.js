// src/adapters/SnowflakeAdapter.js
class SnowflakeAdapter {
    constructor() {
        this.snowflakeTypes = {
            string: 'VARCHAR',
            text: 'VARCHAR',
            longtext: 'VARCHAR',
            integer: 'NUMBER(38,0)',
            biginteger: 'NUMBER(19,0)',
            smallinteger: 'NUMBER(5,0)',
            decimal: 'NUMBER',
            float: 'FLOAT',
            double: 'FLOAT',
            boolean: 'BOOLEAN',
            date: 'DATE',
            datetime: 'TIMESTAMP_NTZ',
            timestamp: 'TIMESTAMP_NTZ',
            time: 'TIME',
            year: 'NUMBER(4,0)',
            json: 'VARIANT',
            uuid: 'VARCHAR(36)',
            binary: 'BINARY',
            enum: 'VARCHAR'
        };
    }

    async create(connection, table, data) {
        try {
            const fields = Object.keys(data);
            const values = Object.values(data);
            const placeholders = fields.map((_, index) => `:${index + 1}`).join(',');
            
            const query = `INSERT INTO ${table} (${fields.join(',')}) VALUES (${placeholders})`;
            
            const result = await this._executeQuery(connection, query, values);
            
            if (result.success) {
                // Get the last inserted ID if available
                const lastInsertId = await this._executeQuery(connection, 'SELECT LAST_INSERT_ID() AS id');
                
                return {
                    success: true,
                    data: {
                        insertId: lastInsertId.success ? lastInsertId.data[0]?.ID : null,
                        affectedRows: result.data.rowCount || 0,
                        record: data
                    }
                };
            }
            
            return result;
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
                    return `${key} = :${paramCounter++}`;
                }).join(' AND ');
                query += ` WHERE ${conditions}`;
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
            
            const result = await this._executeQuery(connection, query, queryParams);
            
            return {
                success: result.success,
                data: result.data || [],
                error: result.error
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
            
            let paramCounter = 1;
            const setFields = Object.keys(data).map(key => {
                return `${key} = :${paramCounter++}`;
            }).join(',');
            const setValues = Object.values(data);
            
            const whereFields = Object.keys(where).map(key => {
                return `${key} = :${paramCounter++}`;
            }).join(' AND ');
            const whereValues = Object.values(where);
            
            const query = `UPDATE ${table} SET ${setFields} WHERE ${whereFields}`;
            const queryParams = [...setValues, ...whereValues];
            
            const result = await this._executeQuery(connection, query, queryParams);
            
            return {
                success: result.success,
                data: {
                    affectedRows: result.data?.rowCount || 0
                },
                error: result.error
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
            
            let paramCounter = 1;
            const whereFields = Object.keys(params.where).map(key => {
                return `${key} = :${paramCounter++}`;
            }).join(' AND ');
            const whereValues = Object.values(params.where);
            
            const query = `DELETE FROM ${table} WHERE ${whereFields}`;
            
            const result = await this._executeQuery(connection, query, whereValues);
            
            return {
                success: result.success,
                data: {
                    affectedRows: result.data?.rowCount || 0
                },
                error: result.error
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
            let query = `SELECT EXISTS(SELECT 1 FROM ${table}`;
            let queryParams = [];
            let paramCounter = 1;
            
            if (params.where) {
                const conditions = Object.keys(params.where).map(key => {
                    queryParams.push(params.where[key]);
                    return `${key} = :${paramCounter++}`;
                }).join(' AND ');
                query += ` WHERE ${conditions}`;
            }
            
            query += ') AS record_exists';
            
            const result = await this._executeQuery(connection, query, queryParams);
            
            return {
                success: result.success,
                data: result.data?.[0]?.RECORD_EXISTS || false,
                error: result.error
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
            const result = await this._executeQuery(connection, queryString, params);
            
            return {
                success: result.success,
                data: result.data || [],
                error: result.error
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
            // Test the connection with a simple query
            const result = await this._executeQuery(connection, 'SELECT CURRENT_DATABASE() AS db_name');
            
            if (result.success) {
                return {
                    success: true,
                    data: `Database initialized successfully. Connected to: ${result.data[0]?.DB_NAME}`
                };
            }
            
            return result;
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
            
            // Snowflake has a limit on the number of expressions in a single INSERT
            // Split into chunks if necessary
            const chunkSize = 1000;
            let affectedRows = 0;
            
            for (let i = 0; i < records.length; i += chunkSize) {
                const chunk = records.slice(i, i + chunkSize);
                const chunkPlaceholders = chunk.map((_, index) => {
                    return `(${fields.map((_, fieldIndex) => 
                        `:${i * fields.length + index * fields.length + fieldIndex + 1}`
                    ).join(',')})`;
                }).join(',');
                
                const query = `INSERT INTO ${table} (${fields.join(',')}) VALUES ${chunkPlaceholders}`;
                const allValues = [];
                chunk.forEach(record => {
                    allValues.push(...Object.values(record));
                });
                
                const result = await this._executeQuery(connection, query, allValues);
                if (result.success && result.data) {
                    affectedRows += result.data.rowCount || 0;
                }
            }
            
            return {
                success: true,
                data: {
                    affectedRows
                }
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async upsert(connection, table, data, conflictFields = []) {
        try {
            const fields = Object.keys(data);
            const values = Object.values(data);
            
            // Snowflake uses MERGE for upsert operations
            const placeholders = fields.map((field, index) => `:${index + 1} AS ${field}`).join(', ');
            const onClause = conflictFields.map(field => `target.${field} = source.${field}`).join(' AND ');
            const updateClause = fields
                .filter(field => !conflictFields.includes(field))
                .map(field => `target.${field} = source.${field}`)
                .join(', ');
            const insertFields = fields.join(', ');
            const insertValues = fields.map(field => `source.${field}`).join(', ');
            
            let query = `
                MERGE INTO ${table} AS target
                USING (SELECT ${placeholders}) AS source
                ON ${onClause}
            `;
            
            if (updateClause) {
                query += `
                    WHEN MATCHED THEN
                        UPDATE SET ${updateClause}
                `;
            }
            
            query += `
                WHEN NOT MATCHED THEN
                    INSERT (${insertFields})
                    VALUES (${insertValues})
            `;
            
            const result = await this._executeQuery(connection, query, values);
            
            return {
                success: result.success,
                data: {
                    affectedRows: result.data?.rowCount || 0
                },
                error: result.error
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
            const result = await this._executeQuery(connection, 'BEGIN TRANSACTION');
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

    async commit(connection) {
        try {
            const result = await this._executeQuery(connection, 'COMMIT');
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

    async rollback(connection) {
        try {
            const result = await this._executeQuery(connection, 'ROLLBACK');
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

    // Snowflake-specific features
    async switchWarehouse(connection, warehouse) {
        try {
            const query = `USE WAREHOUSE ${warehouse}`;
            const result = await this._executeQuery(connection, query);
            
            return {
                success: result.success,
                data: result.success ? `Switched to warehouse: ${warehouse}` : null,
                error: result.error
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async getCurrentWarehouse(connection) {
        try {
            const query = 'SELECT CURRENT_WAREHOUSE() AS warehouse_name';
            const result = await this._executeQuery(connection, query);
            
            return {
                success: result.success,
                data: result.data?.[0]?.WAREHOUSE_NAME || null,
                error: result.error
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async switchSchema(connection, schema) {
        try {
            const query = `USE SCHEMA ${schema}`;
            const result = await this._executeQuery(connection, query);
            
            return {
                success: result.success,
                data: result.success ? `Switched to schema: ${schema}` : null,
                error: result.error
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async getCurrentSchema(connection) {
        try {
            const query = 'SELECT CURRENT_SCHEMA() AS schema_name';
            const result = await this._executeQuery(connection, query);
            
            return {
                success: result.success,
                data: result.data?.[0]?.SCHEMA_NAME || null,
                error: result.error
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    // Data type conversion for Snowflake
    getDataType(type) {
        return this.snowflakeTypes[type] || 'VARCHAR';
    }

    // Clone table (Snowflake-specific feature)
    async cloneTable(connection, sourceTable, targetTable, copyData = true) {
        try {
            const query = `CREATE TABLE ${targetTable} CLONE ${sourceTable}${copyData ? '' : ' COPY GRANTS'}`;
            const result = await this._executeQuery(connection, query);
            
            return {
                success: result.success,
                data: result.success ? `Table ${targetTable} cloned from ${sourceTable}` : null,
                error: result.error
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    // Time travel queries (Snowflake-specific feature)
    async queryTimeTravelBefore(connection, table, timestamp, params = {}) {
        try {
            let query = `SELECT * FROM ${table} BEFORE (TIMESTAMP => '${timestamp}')`;
            let queryParams = [];
            let paramCounter = 1;
            
            if (params.where) {
                const conditions = Object.keys(params.where).map(key => {
                    queryParams.push(params.where[key]);
                    return `${key} = :${paramCounter++}`;
                }).join(' AND ');
                query += ` WHERE ${conditions}`;
            }
            
            const result = await this._executeQuery(connection, query, queryParams);
            
            return {
                success: result.success,
                data: result.data || [],
                error: result.error
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    // Storage integration operations
    async createStage(connection, stageName, url, credentials = {}) {
        try {
            let query = `CREATE STAGE ${stageName} URL='${url}'`;
            
            if (credentials.aws_key_id && credentials.aws_secret_key) {
                query += ` CREDENTIALS=(AWS_KEY_ID='${credentials.aws_key_id}' AWS_SECRET_KEY='${credentials.aws_secret_key}')`;
            }
            
            if (credentials.azure_sas_token) {
                query += ` CREDENTIALS=(AZURE_SAS_TOKEN='${credentials.azure_sas_token}')`;
            }
            
            const result = await this._executeQuery(connection, query);
            
            return {
                success: result.success,
                data: result.success ? `Stage ${stageName} created successfully` : null,
                error: result.error
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    // COPY INTO operations for bulk loading
    async copyInto(connection, table, stageName, filePattern = null, options = {}) {
        try {
            let query = `COPY INTO ${table} FROM @${stageName}`;
            
            if (filePattern) {
                query += `/${filePattern}`;
            }
            
            if (options.fileFormat) {
                query += ` FILE_FORMAT = ${options.fileFormat}`;
            }
            
            if (options.onError) {
                query += ` ON_ERROR = '${options.onError}'`; // CONTINUE, SKIP_FILE, etc.
            }
            
            if (options.force) {
                query += ` FORCE = TRUE`;
            }
            
            const result = await this._executeQuery(connection, query);
            
            return {
                success: result.success,
                data: result.data || [],
                error: result.error
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    // Variant/JSON operations
    async queryVariant(connection, table, variantColumn, path, params = {}) {
        try {
            let query = `SELECT * FROM ${table} WHERE ${variantColumn}:${path} IS NOT NULL`;
            let queryParams = [];
            let paramCounter = 1;
            
            if (params.where) {
                const conditions = Object.keys(params.where).map(key => {
                    queryParams.push(params.where[key]);
                    return `${key} = :${paramCounter++}`;
                }).join(' AND ');
                query += ` AND ${conditions}`;
            }
            
            const result = await this._executeQuery(connection, query, queryParams);
            
            return {
                success: result.success,
                data: result.data || [],
                error: result.error
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    // Information schema queries
    async listTables(connection, databaseName = null, schemaName = null) {
        try {
            let query = `
                SELECT table_name, table_type 
                FROM information_schema.tables 
                WHERE table_type = 'BASE TABLE'
            `;
            
            if (databaseName) {
                query += ` AND table_catalog = '${databaseName}'`;
            }
            
            if (schemaName) {
                query += ` AND table_schema = '${schemaName}'`;
            }
            
            const result = await this._executeQuery(connection, query);
            
            return {
                success: result.success,
                data: result.data || [],
                error: result.error
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    // Helper method to execute queries with proper error handling
    async _executeQuery(connection, query, binds = []) {
        return new Promise((resolve, reject) => {
            try {
                connection.connection.execute({
                    sqlText: query,
                    binds: binds,
                    streamResult: false,
                    complete: (err, statement, rows) => {
                        if (err) {
                            reject(err);
                        } else {
                            resolve({
                                success: true,
                                data: rows,
                                rowCount: statement?.getNumRowsInserted?.() || 
                                         statement?.getNumRowsUpdated?.() || 
                                         statement?.getNumRowsDeleted?.() || 
                                         0,
                                statement: statement
                            });
                        }
                    }
                });
            } catch (error) {
                reject(error);
            }
        });
    }

    // Resource management
    async listWarehouses(connection) {
        try {
            const query = 'SHOW WAREHOUSES';
            const result = await this._executeQuery(connection, query);
            
            return {
                success: result.success,
                data: result.data || [],
                error: result.error
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async createWarehouse(connection, warehouseName, options = {}) {
        try {
            let query = `CREATE WAREHOUSE ${warehouseName}`;
            
            if (options.size) {
                query += ` WITH WAREHOUSE_SIZE = '${options.size}'`;
            }
            
            if (options.autoSuspend) {
                query += ` AUTO_SUSPEND = ${options.autoSuspend}`;
            }
            
            if (options.autoResume !== undefined) {
                query += ` AUTO_RESUME = ${options.autoResume ? 'TRUE' : 'FALSE'}`;
            }
            
            const result = await this._executeQuery(connection, query);
            
            return {
                success: result.success,
                data: result.success ? `Warehouse ${warehouseName} created successfully` : null,
                error: result.error
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    // Account usage and monitoring
    async getQueryHistory(connection, options = {}) {
        try {
            const {
                startTime,
                endTime,
                userName,
                warehouseName,
                databaseName,
                limit = 100
            } = options;
            
            let query = `
                SELECT 
                    query_id,
                    query_text,
                    user_name,
                    warehouse_name,
                    start_time,
                    end_time,
                    total_elapsed_time/1000 as duration_seconds,
                    rows_produced
                FROM snowflake.account_usage.query_history
                WHERE 1=1
            `;
            
            if (startTime) {
                query += ` AND start_time >= '${startTime}'`;
            }
            
            if (endTime) {
                query += ` AND end_time <= '${endTime}'`;
            }
            
            if (userName) {
                query += ` AND user_name = '${userName}'`;
            }
            
            if (warehouseName) {
                query += ` AND warehouse_name = '${warehouseName}'`;
            }
            
            if (databaseName) {
                query += ` AND database_name = '${databaseName}'`;
            }
            
            query += ` ORDER BY start_time DESC LIMIT ${limit}`;
            
            const result = await this._executeQuery(connection, query);
            
            return {
                success: result.success,
                data: result.data || [],
                error: result.error
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }
}

module.exports = SnowflakeAdapter;