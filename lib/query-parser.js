'use strict';
const toFactory = require('tofactory'),
      utils     = require('./utils'),
      isObject  = utils.isObject,
      isRegExp  = utils.isRegExp,
      isArray   = utils.isArray,
      isString  = utils.isString,
      ObjKeys   = Object.keys;

module.exports = toFactory(QueryParser);

function QueryParser(queryObj) {
    let queryFunction;
    
    const operators = {
        '$eq': function (key, value) {
            const compare = generateValueComparator(value);
            
            return function (doc) {
                return compare(doc[key]);
            };
        },
        '$gt': function (key, value) {
            return function (doc) {
                return doc[key] > value;
            };
        },
        '$gte': function (key, value) {
            return function (doc) {
                return doc[key] >= value;  
            };
        },
        '$lt': function (key, value) {
            return function (doc) {
                return doc[key] < value;
            };
        },
        '$lte': function (key, value) {
            return function (doc) {
                return doc[key] <= value;
            };
        },
        '$ne': function (key, value) {
            return function (doc) {
                return doc[key] !== value;
            };
        },
        '$in': function (values) {
            const has = function (val) {
                return values.some(function (item) {
                    return val === item;
                });
            };
            
            return function (value) {
                if (isArray(value)) {
                    return value.some(function (val) {
                        return has(val);
                    });
                }
                return has(value);
            };
        },
        '$nin': function (values) {
            const notHas = function (val) {
                return values.every(function (item) {
                    return val !== item;
                });
            };
            
            return function (value) {
                if (isArray(value)) {
                    return value.every(function (item) {
                        return notHas(item);
                    });
                }
                return notHas(value);
            };
        },
        
        '$and': function (key, value) {
            const queryFunctions = [];
            
            if (isString(key)) {
                ObjKeys(value).forEach(function (k, i) {
                    queryFunctions[i] = operators[k](key, value[k]);
                });
            } else {
                key.forEach(function (obj) {
                    ObjKeys(obj).forEach(function (k) {
                        queryFunctions.push(parse(k, obj[k]));
                    });
                });
            }
            
            return $andFunction(queryFunctions);
        }
    };
    
    
    if (isObject(queryObj)) queryFunction = compile(queryObj);
    
    return {
        compile,
        match
    };
    
    function compile(queryObj) {
        const queryFunctions = [];
        
        ObjKeys(queryObj).forEach(function (key, i) {
            queryFunctions[i] = parse(key, queryObj[key]);
        });
        
        
        return $andFunction(queryFunctions);
    }
    
    function parse(key, value) {
        const op = operators[key];
        
        if (op) return op(key, value);
        
        if (isObject(value)) return operators.$and(key, value);  
        
        return operators.$eq(key, value);
    }
    
    function match(doc) {
        return queryFunction(doc);
    }
    
    function $andFunction(queryFunctions) {
        return function (doc) {
            return queryFunctions.every(function (queryFn) {
                return queryFn(doc); 
            });   
        };
    }
    
    function generateValueComparator(value) {
        if (isRegExp(value)) {
            return function compare(val) {
                return value.test(val);
            };
        }
        
        return function compare(val) {
            return val === value;
        };
    }
    
}