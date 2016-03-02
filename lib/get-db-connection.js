'use strict';
const MongoClient = require('mongodb').MongoClient,
      connOptions = {
        server: {
            poolSize: 10,
            socketOptions: {
                autoReconnect: true,
                noDelay: true,
                keepAlive: 1000,
                connectTimeoutMS: 30000
            }
        }
      };


module.exports = getDbConnection;


function getDbConnection(spec, next) {
    if (!spec.db) {
        let host    = spec.host,
            port    = spec.port,
            dbName  = spec.dbName,
            dbURL   = spec.dbURL || `mongodb://${host}:${port}/${dbName}`,
            options = spec.connOptions || connOptions;
            
        MongoClient.connect(dbURL, options, function (err, dbConn) {
            if (err) return next(err);
            spec.db = dbConn;
            next(undefined, dbConn);
        });
    } else {
        next(undefined, spec.db);
    }
}