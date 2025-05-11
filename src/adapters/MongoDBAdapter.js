// src/adapters/MongoDBAdapter.js
const { ObjectId } = require('mongodb');

class MongoDBAdapter {
    async create(connection, collection, data) {
        try {
            const db = connection.client.db(connection.database);
            const coll = db.collection(collection);
            
            const result = await coll.insertOne(data);
            
            return {
                success: true,
                data: {
                    insertedId: result.insertedId,
                    record: { ...data, _id: result.insertedId }
                }
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async read(connection, collection, params = {}) {
        try {
            const db = connection.client.db(connection.database);
            const coll = db.collection(collection);
            
            // Process query conditions
            let query = params.where || {};
            
            // Convert _id field to ObjectId if present
            if (query._id && typeof query._id === 'string') {
                query._id = new ObjectId(query._id);
            }
            
            // Build the find query
            let cursor = coll.find(query);
            
            // Apply sorting if specified
            if (params.orderBy) {
                const sortOrder = params.order === 'DESC' ? -1 : 1;
                cursor = cursor.sort({ [params.orderBy]: sortOrder });
            }
            
            // Apply pagination if specified
            if (params.offset) {
                cursor = cursor.skip(parseInt(params.offset));
            }
            
            if (params.limit) {
                cursor = cursor.limit(parseInt(params.limit));
            }
            
            const documents = await cursor.toArray();
            
            return {
                success: true,
                data: documents
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async update(connection, collection, params) {
        try {
            const { where, data } = params;
            
            if (!where || !data) {
                throw new Error('Both where and data parameters are required for update');
            }
            
            const db = connection.client.db(connection.database);
            const coll = db.collection(collection);
            
            // Process the where clause
            let filter = where;
            if (filter._id && typeof filter._id === 'string') {
                filter._id = new ObjectId(filter._id);
            }
            
            // Process the update data
            const updateDoc = { $set: data };
            
            const result = await coll.updateMany(filter, updateDoc);
            
            return {
                success: true,
                data: {
                    matchedCount: result.matchedCount,
                    modifiedCount: result.modifiedCount,
                    acknowledged: result.acknowledged
                }
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async updateOne(connection, collection, params) {
        try {
            const { where, data } = params;
            
            if (!where || !data) {
                throw new Error('Both where and data parameters are required for update');
            }
            
            const db = connection.client.db(connection.database);
            const coll = db.collection(collection);
            
            // Process the where clause
            let filter = where;
            if (filter._id && typeof filter._id === 'string') {
                filter._id = new ObjectId(filter._id);
            }
            
            // Process the update data
            const updateDoc = { $set: data };
            
            const result = await coll.updateOne(filter, updateDoc);
            
            return {
                success: true,
                data: {
                    matchedCount: result.matchedCount,
                    modifiedCount: result.modifiedCount,
                    acknowledged: result.acknowledged
                }
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async delete(connection, collection, params) {
        try {
            if (!params.where) {
                throw new Error('Where parameter is required for delete');
            }
            
            const db = connection.client.db(connection.database);
            const coll = db.collection(collection);
            
            // Process the where clause
            let filter = params.where;
            if (filter._id && typeof filter._id === 'string') {
                filter._id = new ObjectId(filter._id);
            }
            
            const result = await coll.deleteMany(filter);
            
            return {
                success: true,
                data: {
                    deletedCount: result.deletedCount,
                    acknowledged: result.acknowledged
                }
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async deleteOne(connection, collection, params) {
        try {
            if (!params.where) {
                throw new Error('Where parameter is required for delete');
            }
            
            const db = connection.client.db(connection.database);
            const coll = db.collection(collection);
            
            // Process the where clause
            let filter = params.where;
            if (filter._id && typeof filter._id === 'string') {
                filter._id = new ObjectId(filter._id);
            }
            
            const result = await coll.deleteOne(filter);
            
            return {
                success: true,
                data: {
                    deletedCount: result.deletedCount,
                    acknowledged: result.acknowledged
                }
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async exists(connection, collection, params) {
        try {
            const db = connection.client.db(connection.database);
            const coll = db.collection(collection);
            
            // Process query conditions
            let query = params.where || {};
            
            if (query._id && typeof query._id === 'string') {
                query._id = new ObjectId(query._id);
            }
            
            const count = await coll.countDocuments(query, { limit: 1 });
            
            return {
                success: true,
                data: count > 0
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async query(connection, pipeline, options = {}) {
        try {
            const db = connection.client.db(connection.database);
            
            // For MongoDB, we use aggregation pipelines as the "query"
            // The pipeline parameter should be an array of aggregation stages
            
            let result;
            
            if (Array.isArray(pipeline)) {
                // Aggregation pipeline
                const collection = options.collection || options.from;
                if (!collection) {
                    throw new Error('Collection name must be specified for aggregation');
                }
                
                const coll = db.collection(collection);
                result = await coll.aggregate(pipeline).toArray();
            } else if (typeof pipeline === 'object' && pipeline.collection) {
                // Raw MongoDB command
                result = await db.command(pipeline);
            } else {
                throw new Error('Invalid query format for MongoDB');
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
            const db = connection.client.db(connection.database);
            
            // Test the connection
            await db.admin().ping();
            
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
    async bulkInsert(connection, collection, documents) {
        try {
            if (!documents.length) {
                return { success: true, data: { insertedCount: 0 } };
            }
            
            const db = connection.client.db(connection.database);
            const coll = db.collection(collection);
            
            const result = await coll.insertMany(documents);
            
            return {
                success: true,
                data: {
                    insertedCount: result.insertedCount,
                    insertedIds: result.insertedIds
                }
            };
        } catch (error) {
            return {
                success: false,
                error: error.message
            };
        }
    }

    async upsert(connection, collection, filter, data, options = {}) {
        try {
            const db = connection.client.db(connection.database);
            const coll = db.collection(collection);
            
            // Process filter
            if (filter._id && typeof filter._id === 'string') {
                filter._id = new ObjectId(filter._id);
            }
            
            const updateDoc = { $set: data };
            const updateOptions = { upsert: true, ...options };
            
            const result = await coll.updateOne(filter, updateDoc, updateOptions);
            
            return {
                success: true,
                data: {
                    matchedCount: result.matchedCount,
                    modifiedCount: result.modifiedCount,
                    upsertedCount: result.upsertedCount,
                    upsertedId: result.upsertedId,
                    acknowledged: result.acknowledged
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
            const session = connection.client.startSession();
            session.startTransaction();
            
            return { 
                success: true, 
                session 
            };
        } catch (error) {
            return { 
                success: false, 
                error: error.message 
            };
        }
    }

    async commit(session) {
        try {
            await session.commitTransaction();
            session.endSession();
            return { success: true };
        } catch (error) {
            return { 
                success: false, 
                error: error.message 
            };
        }
    }

    async rollback(session) {
        try {
            await session.abortTransaction();
            session.endSession();
            return { success: true };
        } catch (error) {
            return { 
                success: false, 
                error: error.message 
            };
        }
    }

    // Collection management
    async createCollection(connection, collectionName, options = {}) {
        try {
            const db = connection.client.db(connection.database);
            const result = await db.createCollection(collectionName, options);
            
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

    async dropCollection(connection, collectionName) {
        try {
            const db = connection.client.db(connection.database);
            const result = await db.collection(collectionName).drop();
            
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

    async createIndex(connection, collection, index, options = {}) {
        try {
            const db = connection.client.db(connection.database);
            const coll = db.collection(collection);
            
            const result = await coll.createIndex(index, options);
            
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
}

module.exports = MongoDBAdapter;