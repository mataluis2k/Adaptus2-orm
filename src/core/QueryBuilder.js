// src/core/QueryBuilder.js
class QueryBuilder {
    constructor() {
        this.reset();
    }

    reset() {
        this.queryType = null;
        this.tableName = null;
        this.selectFields = ['*'];
        this.whereConditions = [];
        this.joinClauses = [];
        this.groupByFields = [];
        this.havingConditions = [];
        this.orderByFields = [];
        this.limitValue = null;
        this.offsetValue = null;
        this.insertData = null;
        this.updateData = null;
        this.params = [];
        this.databaseType = null;
        return this;
    }

    database(type) {
        this.databaseType = type.toLowerCase();
        return this;
    }

    table(name) {
        this.tableName = name;
        return this;
    }

    select(fields = '*') {
        this.queryType = 'SELECT';
        if (Array.isArray(fields)) {
            this.selectFields = fields;
        } else if (typeof fields === 'string') {
            this.selectFields = fields === '*' ? ['*'] : [fields];
        }
        return this;
    }

    insert(data) {
        this.queryType = 'INSERT';
        this.insertData = data;
        return this;
    }

    update(data) {
        this.queryType = 'UPDATE';
        this.updateData = data;
        return this;
    }

    delete() {
        this.queryType = 'DELETE';
        return this;
    }

    where(field, operator = '=', value = null) {
        // Support for object syntax: .where({ field: value })
        if (typeof field === 'object' && field !== null) {
            Object.entries(field).forEach(([key, val]) => {
                this.whereConditions.push({
                    field: key,
                    operator: '=',
                    value: val,
                    type: 'AND'
                });
                this.params.push(val);
            });
        } else {
            // Handle different argument patterns
            if (arguments.length === 2 && operator !== '=' && operator !== '!=' && operator !== '>' && operator !== '<' && operator !== '>=' && operator !== '<=') {
                // Two arguments: field and value (default operator is =)
                value = operator;
                operator = '=';
            }
            
            this.whereConditions.push({
                field: field,
                operator: operator,
                value: value,
                type: 'AND'
            });
            this.params.push(value);
        }
        return this;
    }

    orWhere(field, operator = '=', value = null) {
        if (arguments.length === 2) {
            value = operator;
            operator = '=';
        }
        
        this.whereConditions.push({
            field: field,
            operator: operator,
            value: value,
            type: 'OR'
        });
        this.params.push(value);
        return this;
    }

    whereIn(field, values) {
        this.whereConditions.push({
            field: field,
            operator: 'IN',
            value: values,
            type: 'AND'
        });
        this.params.push(...values);
        return this;
    }

    whereNotIn(field, values) {
        this.whereConditions.push({
            field: field,
            operator: 'NOT IN',
            value: values,
            type: 'AND'
        });
        this.params.push(...values);
        return this;
    }

    whereLike(field, pattern) {
        this.whereConditions.push({
            field: field,
            operator: 'LIKE',
            value: pattern,
            type: 'AND'
        });
        this.params.push(pattern);
        return this;
    }

    whereNull(field) {
        this.whereConditions.push({
            field: field,
            operator: 'IS NULL',
            value: null,
            type: 'AND'
        });
        return this;
    }

    whereNotNull(field) {
        this.whereConditions.push({
            field: field,
            operator: 'IS NOT NULL',
            value: null,
            type: 'AND'
        });
        return this;
    }

    join(table, firstField, operator = '=', secondField = null) {
        if (arguments.length === 3) {
            secondField = operator;
            operator = '=';
        }
        
        this.joinClauses.push({
            type: 'INNER',
            table: table,
            firstField: firstField,
            operator: operator,
            secondField: secondField
        });
        return this;
    }

    leftJoin(table, firstField, operator = '=', secondField = null) {
        if (arguments.length === 3) {
            secondField = operator;
            operator = '=';
        }
        
        this.joinClauses.push({
            type: 'LEFT',
            table: table,
            firstField: firstField,
            operator: operator,
            secondField: secondField
        });
        return this;
    }

    rightJoin(table, firstField, operator = '=', secondField = null) {
        if (arguments.length === 3) {
            secondField = operator;
            operator = '=';
        }
        
        this.joinClauses.push({
            type: 'RIGHT',
            table: table,
            firstField: firstField,
            operator: operator,
            secondField: secondField
        });
        return this;
    }

    groupBy(fields) {
        if (Array.isArray(fields)) {
            this.groupByFields = fields;
        } else {
            this.groupByFields = [fields];
        }
        return this;
    }

    having(field, operator = '=', value = null) {
        if (arguments.length === 2) {
            value = operator;
            operator = '=';
        }
        
        this.havingConditions.push({
            field: field,
            operator: operator,
            value: value,
            type: 'AND'
        });
        this.params.push(value);
        return this;
    }

    orderBy(field, direction = 'ASC') {
        this.orderByFields.push({
            field: field,
            direction: direction.toUpperCase()
        });
        return this;
    }

    limit(value) {
        this.limitValue = parseInt(value);
        return this;
    }

    offset(value) {
        this.offsetValue = parseInt(value);
        return this;
    }

    toSQL() {
        if (!this.databaseType) {
            throw new Error('Database type must be specified using .database() method');
        }

        switch (this.queryType) {
            case 'SELECT':
                return this._buildSelectQuery();
            case 'INSERT':
                return this._buildInsertQuery();
            case 'UPDATE':
                return this._buildUpdateQuery();
            case 'DELETE':
                return this._buildDeleteQuery();
            default:
                throw new Error('Query type must be specified');
        }
    }

    _buildSelectQuery() {
        let query = `SELECT ${this.selectFields.join(', ')} FROM ${this.tableName}`;

        // Add JOINs
        this.joinClauses.forEach(join => {
            query += ` ${join.type} JOIN ${join.table} ON ${join.firstField} ${join.operator} ${join.secondField}`;
        });

        // Add WHERE clauses
        if (this.whereConditions.length > 0) {
            query += ' WHERE ';
            const whereString = this._buildWhereClause();
            query += whereString;
        }

        // Add GROUP BY
        if (this.groupByFields.length > 0) {
            query += ` GROUP BY ${this.groupByFields.join(', ')}`;
        }

        // Add HAVING
        if (this.havingConditions.length > 0) {
            query += ' HAVING ';
            const havingString = this._buildHavingClause();
            query += havingString;
        }

        // Add ORDER BY
        if (this.orderByFields.length > 0) {
            const orderString = this.orderByFields.map(item => 
                `${item.field} ${item.direction}`
            ).join(', ');
            query += ` ORDER BY ${orderString}`;
        }

        // Add LIMIT and OFFSET
        if (this.limitValue !== null) {
            query += ` LIMIT ${this.limitValue}`;
            if (this.offsetValue !== null) {
                query += ` OFFSET ${this.offsetValue}`;
            }
        }

        return {
            sql: query,
            params: this.params
        };
    }

    _buildInsertQuery() {
        if (!this.insertData) {
            throw new Error('Insert data must be specified');
        }

        const fields = Object.keys(this.insertData);
        const values = Object.values(this.insertData);
        
        let placeholders;
        switch (this.databaseType) {
            case 'mysql':
                placeholders = fields.map(() => '?').join(', ');
                break;
            case 'postgresql':
                placeholders = fields.map((_, index) => `$${index + 1}`).join(', ');
                break;
            case 'snowflake':
                placeholders = fields.map((_, index) => `:${index + 1}`).join(', ');
                break;
            default:
                placeholders = fields.map(() => '?').join(', ');
        }

        const query = `INSERT INTO ${this.tableName} (${fields.join(', ')}) VALUES (${placeholders})`;

        return {
            sql: query,
            params: values
        };
    }

    _buildUpdateQuery() {
        if (!this.updateData) {
            throw new Error('Update data must be specified');
        }

        const fields = Object.keys(this.updateData);
        const values = Object.values(this.updateData);
        
        let setClause;
        switch (this.databaseType) {
            case 'mysql':
                setClause = fields.map(field => `${field} = ?`).join(', ');
                break;
            case 'postgresql':
                setClause = fields.map((field, index) => `${field} = $${index + 1}`).join(', ');
                break;
            case 'snowflake':
                setClause = fields.map((field, index) => `${field} = :${index + 1}`).join(', ');
                break;
            default:
                setClause = fields.map(field => `${field} = ?`).join(', ');
        }

        let query = `UPDATE ${this.tableName} SET ${setClause}`;

        // Add WHERE clauses
        if (this.whereConditions.length > 0) {
            query += ' WHERE ';
            const whereString = this._buildWhereClause();
            query += whereString;
        }

        return {
            sql: query,
            params: [...values, ...this.params]
        };
    }

    _buildDeleteQuery() {
        let query = `DELETE FROM ${this.tableName}`;

        // Add WHERE clauses
        if (this.whereConditions.length > 0) {
            query += ' WHERE ';
            const whereString = this._buildWhereClause();
            query += whereString;
        }

        return {
            sql: query,
            params: this.params
        };
    }

    _buildWhereClause() {
        let whereString = '';
        let paramCounter = 0;

        this.whereConditions.forEach((condition, index) => {
            if (index > 0) {
                whereString += ` ${condition.type} `;
            }

            if (condition.operator === 'IN' || condition.operator === 'NOT IN') {
                const placeholderCount = condition.value.length;
                let placeholders;
                
                switch (this.databaseType) {
                    case 'mysql':
                        placeholders = Array(placeholderCount).fill('?').join(', ');
                        break;
                    case 'postgresql':
                        placeholders = Array(placeholderCount).fill().map((_, i) => `$${paramCounter + i + 1}`).join(', ');
                        break;
                    case 'snowflake':
                        placeholders = Array(placeholderCount).fill().map((_, i) => `:${paramCounter + i + 1}`).join(', ');
                        break;
                    default:
                        placeholders = Array(placeholderCount).fill('?').join(', ');
                }
                
                whereString += `${condition.field} ${condition.operator} (${placeholders})`;
                paramCounter += placeholderCount;
            } else if (condition.operator === 'IS NULL' || condition.operator === 'IS NOT NULL') {
                whereString += `${condition.field} ${condition.operator}`;
            } else {
                let placeholder;
                switch (this.databaseType) {
                    case 'mysql':
                        placeholder = '?';
                        break;
                    case 'postgresql':
                        placeholder = `$${paramCounter + 1}`;
                        break;
                    case 'snowflake':
                        placeholder = `:${paramCounter + 1}`;
                        break;
                    default:
                        placeholder = '?';
                }
                
                whereString += `${condition.field} ${condition.operator} ${placeholder}`;
                paramCounter++;
            }
        });

        return whereString;
    }

    _buildHavingClause() {
        let havingString = '';
        this.havingConditions.forEach((condition, index) => {
            if (index > 0) {
                havingString += ` ${condition.type} `;
            }
            havingString += `${condition.field} ${condition.operator} ${condition.value}`;
        });
        return havingString;
    }

    // Convenience methods for common queries
    static create(databaseType) {
        return new QueryBuilder().database(databaseType);
    }

    get() {
        return this.toSQL();
    }

    first() {
        return this.limit(1).toSQL();
    }

    count(field = '*') {
        this.selectFields = [`COUNT(${field}) as count`];
        return this.toSQL();
    }

    exists() {
        return this.limit(1).count().toSQL();
    }
}

module.exports = QueryBuilder;