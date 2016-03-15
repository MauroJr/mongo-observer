'use strict';

const debug                     = require('debug')('mongo-observer'),
      Oplog                     = require('./oplog'),
      Emitter                   = require('create-emitter'),
      MongoDriver               = require('./mongo-driver'),
      utils                     = require('./utils'),
      isFunction                = utils.isFunction,
      isUndefined               = utils.isUndefined,
      isEmpty                   = utils.isEmpty,
      isString                  = utils.isString,
      isArrayOfStrings          = utils.isArrayOfStrings,
      
      ERR_ALREADY_LISTENING   = new Error('MongoObserver already listening'),
      ERR_CREATION_TIMEOUT    = new Error('MongoObserver creation timeout!'),
      ERR_ON_CHANGE_OBS_COLS  = new Error(
          '"collections" parameter must be a array of strings');
      

module.exports = require('tofactory')(MongoObserver);


function MongoObserver(spec) {
    const emitter   = Emitter.create(),
          INSERT    = 'isert', 
          UPDATE    = 'update', 
          REMOVE    = 'remove';
    
    let count = 0,
        listening = false, 
        oplog, mongo, db;
    
    init();
    
    return {
        driver: mongo.driver,
        getDbConn: mongo.getDbConn,
        collection,
        get observableColletions() {
            return oplog.collections;
        },
        set observableColletions(collections) {
            if (!collections || isEmpty(collections)) {
                getAllCollectionNames(function (err, collectionNames) {
                    if (err) return emitter.emit('error', err);
                    oplog.collections = collectionNames;
                });
            } else {
                if (isArrayOfStrings(collections)) {
                    oplog.collections = collections;
                } else {
                    emitter.emit('error', ERR_ON_CHANGE_OBS_COLS);
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
        mongo.connect(function (err, dbConn) {
            if (err) throw err;
            
            db = dbConn;
            oplog = Oplog.create({
                db,
                collections: spec.observableColletions || [],
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
        if (listening) return done(ERR_ALREADY_LISTENING);
        if (count > 20) return done(ERR_CREATION_TIMEOUT);
        
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
    
    function collection(name, options, callback) {
        if (!isString(name)) throw new TypeError('"name" must be a string');
        
        if (isObservable(name)) {
            
            if (isUndefined(callback)) {
                if (isFunction(options)) {
                    callback = options;
                    options = undefined;
                }
            }
            
            if (!callback) {
                let collection = db.collection(name, options);
                return getObservableCollection(collection);
            }
            
            db.collection(name, options, function (err, collection) {
                if (err) return callback(err);
                
                callback(undefined, getObservableCollection(collection));
            });
            
        } else {
            return db.collection(name, options, callback);
        }
    }
    
    function getObservableCollection(collection) {
        return Object.freeze(Object.assgn({
            collection,
            find: cursorObserver
        }))
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
    
    function isObservable(collectionName) {
        if (oplog.collections.indexOf(collectionName) !== -1) return true;
        return false;
    }
}
