'use strict';

const debug     = require('debug')('oplog'),
      Timestamp = require('mongodb').Timestamp;

module.exports = require('tofactory')(Oplog);


function Oplog(spec) {
    let dbName, oplogCol, collections, onUpdate, operations;
    const noop = function () {};
    
    init();
    
    return {
        listen
    };
    
    function init() {
        const db = spec.db.db('local');
        
        collections = spec.collections || [];
        dbName = spec.db.databaseName;
        db.on('error', function (e) {console.log('error:', e)});
        db.on('timeout', function (e) {console.log('timeout:', e)});
        db.on('reconnect', function (e) {console.log('reconnect:', e)});
        db.on('parseError', function (e) {console.log('parseError:', e)});
        db.on('close', function (e) {console.log('close:', e)});
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
        const cursor = oplogCol.find({});
        
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
            next();
    
            // And when data arrives at that stream, print it out
            stream.on('data', function(doc) {
                operations[doc.op](doc.ns.split('.')[1], doc.o, doc);
            });
            
            stream.on('error', function (e) {
                console.log('stream error:', e);
                throw new Error('Stream Error');
            });
            
            stream.on('end', function (e) {
                console.log('stream end:', e);
                throw new Error('Stream Ended');
            });
            
            stream.on('close', function (e) {
                console.log('stream close:', e);
                throw new Error('Stream Closed');
            });
        });
    }
    
    function updateOp(collection, data, doc) {
        let _id;
        const operator = Object.keys(data)[0];
        
        if (operator[0] === '$') {
            _id = doc.o2._id;
            switch (operator) {
                case '$set':
                    data = data.$set;
                    break;
                default:
                    data = undefined;
            }
        } else {
            _id = data._id;
        }
        
        onUpdate(collection, data, _id);
    }
}


