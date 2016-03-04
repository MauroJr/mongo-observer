'use strict';

const debug         = require('debug')('mongo-observer'),
      Oplog         = require('./oplog'),
      Emitter       = require('create-emitter'),
      getDbConn     = require('./get-db-connection');


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
        getDbConn(spec, function (err) {
            if (err) throw err;
            db = spec.db;
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
