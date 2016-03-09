'use strict';
const toString  = Object.prototype.toString,
      keys      = Object.keys;

module.exports = Object.freeze({
    isString,
    isFunction,
    isArray,
    isArrayOfStrings,
    isObject,
    isRegExp,
    isUndefined,
    isNull,
    isEmpty,
    isEmptyArray,
    isEmptyObject,
    noop
});

function isString(arg) {
    return toString.call(arg) === '[object String]' ? true : false;
}

function isFunction(arg) {
    return toString.call(arg) === '[object Function]' ? true : false; 
}

function isArray(arg) {
    return arg instanceof Array;
}

function isArrayOfStrings(arg) {
    return allItemsOf(arg, isString);
}

function allItemsOf(array, isType) {
    if (isArray(array)) {
        return array.every(isType);
    }
    return false;
}

function isObject(arg) {
    return toString.call(arg) === '[object Object]' ? true : false; 
}

function isUndefined(arg) {
    return toString.call(arg) === '[object Undefined]' ? true : false;
}

function isNull(arg) {
    return toString.call(arg) === '[object Null]' ? true : false;
}

function isRegExp(arg) {
    return toString.call(arg) === '[object RegExp]' ? true : false;
}

function isEmpty(arg) {
    if (arg) {
        if (isObject(arg)) return isEmptyObject(arg);
        if (isArray(arg)) return isEmptyArray(arg);
        return false;
    }
    return true;
}

function isEmptyArray(arg) {
    return arg.length ? true : false;
}

function isEmptyObject(arg) {
    return keys(arg).length ? true : false;
}

function noop() {}