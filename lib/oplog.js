'use strict';

const debug     = require('debug')('oplog'),
      Timestamp = require('mongodb').Timestamp,
      utils     = require('utils'),
      noop      = utils.noop,
      keys      = Object.keys;

module.exports = require('tofactory')(Oplog);


function Oplog(spec) {
    let dbName, oplogCol, collections, onUpdate, operations, cursor;
    
    init();
    
    return {
        get collections() {
            return collections;
        },
        set collections(collectionNames) {
            collections = collectionNames;
            // closes the cursor which triggers the listen function again
            cursor && cursor.close();
        },
        listen
    };
    
    function init() {
        const db = spec.db.db('local');
        
        collections = spec.collections || [];
        dbName = spec.db.databaseName;
        oplogCol = db.collection("oplog.rs");
        
        operations = {
            i: spec.onInsert || noop,
            u: updateOp,
            d: spec.onRemove || noop
        };
        
        onUpdate = spec.onUpdate || noop;
    }
    
    function listen(next) {
        // Find the highest timestamp
        oplogCol.find({})
            .project({ts: true})
            .sort({$natural: -1})
            .limit(1)
            .next(function (err, oldestChange) {
                if (err) return next(err);
                
                // If there isn't one found, get one from the local clock
                if (oldestChange) {
                    oldestChange = oldestChange.ts;
                } else {
                    oldestChange = new Timestamp(0, 
                        Math.floor(new Date().getTime() / 1000));
                }
                
                handleChangesStream(oldestChange, next);
            });
    }
    
    function handleChangesStream(oldestChange, next) {
        let stream;
        
        // Create a cursor for tailing and set it to await data
        const query = {
            ts: {$gt: oldestChange},
            ns: {$regex: `^${dbName}.(${collections.join('|')})$`}
        };
        
        cursor = oplogCol.find(query, {numberOfRetries: Number.MAX_VALUE})
            .addCursorFlag('tailable', true)
            .addCursorFlag('awaitData', true)
            .addCursorFlag('oplogReplay', true);
            
        // Wrap that cursor in a Node Stream
        stream = cursor.stream();

        stream.on('data', function(doc) {
            try {
                operations[doc.op](doc.ns.split('.')[1], doc.o, doc);
            } catch (e) {
                debug(e);
                debug(doc);
            }
        });
        
        stream.on('error', function (err) {
            debug('stream error');
            debug(err);
            next(err);
        });
        
        stream.on('end', function (e) {
            debug('stream ended');
            debug(e);
            listen(next);
        });
        
        stream.on('close', function (e) {
            debug('stream closed');
            debug(e);
        });
    }
    
    function updateOp(collection, data, doc) {
        let hasOperators = true,
            _id;
        
        const operators = keys(data);
        
        // if data has update operators
        if (operators[0] && operators[0][0] === '$') {
            _id = doc.o2._id;
        } else {
            _id = data._id;
        }
        
        onUpdate(collection, hasOperators, _id, data);
    }
    
}
