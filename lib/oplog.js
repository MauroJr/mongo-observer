'use strict';

const debug     = require('debug')('oplog'),
      Timestamp = require('mongodb').Timestamp,
      utils     = require('utils'),
      noop      = utils.noop;

module.exports = require('tofactory')(Oplog);


function Oplog(spec) {
    let dbName, oplogCol, collections, onUpdate, operations, cursor;
    
    init();
    
    return {
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
        let _id, updatedData, unsurpportedOperator;
        
        
                
        if (operators) {
            _id = doc.o2._id;
            
            operators.forEach(function (operator) {
                switch (operator) {
                    case '$set':
                        if (updatedData) {
                            updatedData = Object.assign(updatedData, data.$set);
                        } else {
                            updatedData = data.$set;
                        }
                        break;
                    case '$unset':
                        updatedData = updatedData || {};
                        
                        Object.keys(data.$unset).forEach(function (key) {
                            updatedData[key] = undefined; 
                        });
                        break;
                    default:
                        unsurpportedOperator = true;
                } 
            });
        } else {
            _id = data._id;
        }
        
        if (unsurpportedOperator) updatedData = undefined; 
        
        onUpdate(collection, updatedData, _id);
    }
    
    
    
    
}
