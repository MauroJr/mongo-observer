'use strict';

const debug                 = require('debug')('mongo-observer:cache'),
      toFactory             = require('tofactory'),
      MongoObserver         = require('mongo-observer'),
      getMongoConnection    = require('./get-db-connection'),
      idFromHexString       = require('mongodb').ObjectID.createFromHexString;


module.exports = toFactory(Cache);


function Cache(spec) {
    const cache = Object.create(null);
          
    let db;
    
    init();
    
    return {
        sync,
        'get': get,
        insert,
        remove
    };
    
    function init() {
        getMongoConnection(spec, function (err, dbConn) {
            if (err) throw err;
            db = dbConn;
        });
    }
    
    function get(collection, id) {
        return getCollection(collection)[id];
    }
    
    function insert(collection, id, data, next) {
        getCollection(collection)[id] = data;
    }
    
    function remove(collection, id) {
        delete getCollection(collection)[id];
    }
    
    function sync(collection, id, data, next) {
        const col = getCollection(collection),
              doc = col[id];
        
        if (doc) {
            col[id] = Object.assign(doc, data);
            next(undefined, data, doc);
        } else {
            findOneById(collection, id, function (err, doc) {
                if (err) return next(err);
                col[id] = doc;
                next(undefined, data, doc);
            });
        }
    }
    
    function findOneById(colName, id, next) {
        if (db) {
            db.collection(colName, function (err, collection) {
                if (err) return next(err);
                collection
                    .find({_id: idFromHexString(id)})
                    .limit(1)
                    .next(function (err, doc) {
                        if (err) return next(err);
                        if (doc) return next(undefined, doc);
                        debug(`doc not found ${id}`);
                    });
            });
        } else {
            next(new Error('no database connection'));
        }
    }
    
    function getCollection(collection) {
        return cache[collection] || (cache[collection] = {});
    }
}