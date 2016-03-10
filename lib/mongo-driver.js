'use strict';
const toFactory     = require('tofactory'),
      mongodb       = require('mongodb'),
      MongoClient   = mongodb.MongoClient;
      


module.exports = toFactory(MongoDriver);


function MongoDriver(spec) {
    let db, url, connOpts;
    
    init();
    
    return {
        driver: mongodb,
        connect,
        getDbConn
    };
    
    function init() {
        if (spec.db) {
            db = spec.db;
            return;
        }
        
        connOpts = spec.connOpts || {
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
        
        if (typeof spec === 'string') {
            url = spec;
        } else {
            url = spec.dbURL;
        }
        
    }
    
    function connect(next) {
        if (db) return next(undefined, db);
        
        MongoClient.connect(url, connOpts, function (err, dbConn) {
            if (err) return next(err);
            db = dbConn;
            next(undefined, db);
        });
    }
    
    function getDbConn() {
        return db;
    }
    
}
