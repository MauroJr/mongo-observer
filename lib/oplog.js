'use strict';

const debug     = require('debug')('oplog'),
      Timestamp = require('mongodb').Timestamp;

module.exports = require('tofactory')(Oplog);


function Oplog(spec) {
    let dbName, oplogCol, collections, onUpdate, operations, cursor;
    const noop = function () {};
    
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
        cursor = oplogCol.find({});
        
        cursor.project({ts: true});
        cursor.sort({$natural: -1});
        cursor.limit(1);
        cursor.next(function (err, result) {
            let lastOplogTime, queryForTime, tstamp, cursor, stream, 
                queryCursor, nsRegex;
        
            if (err) return next(err);
        
            lastOplogTime = result.ts;
            
            // If there isn't one found, get one from the local clock
            if (lastOplogTime) {
                queryForTime = {
                    $gt: lastOplogTime
                };
            } else {
                tstamp = new Timestamp(0, Math.floor(new Date().getTime() / 1000));
                queryForTime = {
                    $gt: tstamp
                };
            }
            
            nsRegex = `^${dbName}.(${collections.join('|')})$`;
            debug('regex:', nsRegex);
            
            // Create a cursor for tailing and set it to await data
            queryCursor = {
                ts: queryForTime,
                ns: {$regex: nsRegex}
            };
            
            cursor = oplogCol.find(queryCursor, 
                {numberOfRetries: Number.MAX_VALUE})
                .addCursorFlag('tailable', true)
                .addCursorFlag('awaitData', true)
                .addCursorFlag('oplogReplay', true);
                
            // Wrap that cursor in a Node Stream
            stream = cursor.stream();
            next && next();
    
            // And when data arrives at that stream, print it out
            stream.on('data', function(doc) {
                try {
                    operations[doc.op](doc.ns.split('.')[1], doc.o, doc);
                } catch (e) {
                    debug(e);
                    debug(doc);
                }
            });
            
            stream.on('error', function (e) {
                debug('stream error');
                debug(e);
            });
            
            stream.on('end', function (e) {
                debug('stream ended');
                debug(e);
            });
            
            stream.on('close', function (e) {
                debug('stream closed');
                debug(e);
                
            });
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
