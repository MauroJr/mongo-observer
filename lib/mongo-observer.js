'use strict';

const MongoClient   = require('mongodb').MongoClient,
      debug         = require('debug')('mongo-observer'),
      Oplog         = require('./oplog'),
      Emitter       = require('create-emitter'),
      connOptions   = {
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


module.exports = require('tofactory')(MongoObserver);


function MongoObserver(spec) {
    const emitter   = Emitter.create(),
          INSERT    = 'isert', 
          UPDATE    = 'update', 
          REMOVE    = 'remove';
    
    let count = 0,
        db, oplog, colOpts;
    
    init();
    
    return {
        on: emitter.on,
        off: emitter.off,
        observe
    };
    
    function init() {
        getDbConn(function (err) {
            if (err) throw err;
            defineObserver();
        });
    }
    
    function observe() {
        if (count > 10) return new Error('Oplog creation timeout!');
        if (oplog) {
            oplog.listen();
        } else {
            setTimeout(observe, 200);
            count += 1;
        }
    }
    
    function insertOp(collection, data) {
        emitter.emit(`${collection}:${INSERT}:${data._id}`, data);
    }
    
    function removeOp(collection, data) {
        emitter.emit(`${collection}:${REMOVE}:${data._id}`);
    }
    
    function updateOp(collection, data, _id) {
        if (data) {
            emitter.emit(`${collection}:${UPDATE}:${_id}`, data);
        } else {
            getDataFromDb(collection, _id);
        }
    }
    
    function getDataFromDb(collection, _id) {
        findOneById(collection, _id, function (err, doc) {
            if (err) return debug(err);
            emitter.emit(`${collection}:${UPDATE}`, doc);
        });
    }
    
    function findOneById(colName, _id, next) {
        db.collection(colName, function (err, col) {
            if (err) return next(err);
            col.find({_id: _id}).limit(1).next(function (err, doc) {
                if (err) return next(err);
                return next(undefined, doc);
            });
        });
    }
    
    function getDbConn(next) {
        db = spec.db;
        if (!db) {
            let host    = spec.host,
                port    = spec.port,
                dbName  = spec.dbName,
                dbURL   = spec.dbURL || `mongodb://${host}:${port}/${dbName}`,
                options = spec.connOptions || connOptions;
                
            MongoClient.connect(dbURL, options, function (err, dbConn) {
                if (err) return next(err);
                db = dbConn;
                spec.db = dbConn;
                next();
            });
        } else {
            next();
        }
    }
    
    function defineObserver() {
        colOpts = spec.collections || [];
        Array.isArray(colOpts) && (colOpts = colOpts.join('|'));
        oplog = Oplog.create(Object.assign(spec, {
            onInsert: insertOp,
            onUpdate: updateOp,
            onRemove: removeOp
        }));
        count = 0;
    }
}
