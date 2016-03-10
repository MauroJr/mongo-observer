'use strict';

const debug                     = require('debug')('mongo-observer'),
      Oplog                     = require('./oplog'),
      Emitter                   = require('create-emitter'),
      MongoDriver               = require('./mongo-driver'),
      utils                     = require('./utils'),
      isFunction                = utils.isFunction,
      isArray                   = utils.isArray,
      isEmpty                   = utils.isEmpty,
      isString                  = utils.isString,
      isArrayOfStrings          = utils.isArrayOfStrings,
      noop                      = utils.noop,
      
      ERROR_ALREADY_LISTENING   = new Error('MongoObserver already listening');
      ERROR_CREATION_TIMEOUT    = new Error('MongoObserver creation timeout!');
      
      
module.exports = require('tofactory')(MongoObserver);


function MongoObserver(spec) {
    const emitter   = Emitter.create(),
          INSERT    = 'isert', 
          UPDATE    = 'update', 
          REMOVE    = 'remove';
    
    let count = 0,
        listening = false, 
        oplog, mongo;
    
    init();
    
    return {
        driver: mongo.driver,
        getDbConn: mongo.getDbConn,
        collection,
        get observebleColletions() {
            return oplog.collections;
        },
        set observebleColletions(cols, done) {
            done = done || noop;
            if (!cols || isFunction(cols) || isEmpty(cols)) {
                getAllCollectionNames(function (err, collectionNames) {
                    if (err) return done(err);
                    oplog.collections = collectionNames;
                    done();
                })
            } else {
                if (isArrayOfStrings(cols)) {
                    oplog.collections = cols;
                } else {
                    done(new Error('"cols" parameter must be a Array'));
                }
            }
        },
        on: emitter.on,
        off: emitter.off,
        observe
    };

    
    function init() {
        let opts;
        
        if (isString(spec)) {
            opts = spec;    
        } else {
            opts = {
                url: spec.dbURL,
                db: spec.db,
                connOpts: spec.dbConnOpts
            };
        }
        
        mongo = MongoDriver(opts);
        mongo.connect(function (err, db) {
            if (err) throw err;
            
            oplog = Oplog.create({
                db,
                collections: spec.observebleColletions || [];
                onInsert: insertOperation,
                onUpdate: updateOperation,
                onRemove: removeOperation
            });
            
            oplog.on('error', function (err) {
                emitter.emit(err);
            });
            
            count = 0;
        });
    }
    
    function observe(done) {
        if (listening) return done(ERROR_ALREADY_LISTENING);
        if (count > 20) return done(ERROR_CREATION_TIMEOUT);
        
        if (oplog) {
            ready();
        } else { // wait for oplog creation
            setTimeout(ready, 200);
            count += 1;
        }
        
        function ready() {
            oplog.listen(function (err) {
                if (err) return done(err);
                listening = true;
                done();
            });
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
    
    function getAllCollectionNames(next) {
        const collectionNames = [];
        
        db.collections(function (err, collections) {
            if (err) return next(err);
            collections.forEach(function (collection, i) {
                 collectionNames[i] = collection.collectionName;
            });
            next(undefined, collectionNames);
        });
    }
}
