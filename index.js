'use strict';

module.exports = {
    create: require('./lib/mongo-observer').create,
    getMongoConnection: require('./lib/get-db-connection'),
    ObjectID: require('mongodb').ObjectID
};