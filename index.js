'use strict';

/**
 * Adds commas to a number
 * @param {number} number
 * @param {string} locale
 * @return {string}
 */
module.exports.testNum = function(number, locale) {
    return number.toLocaleString(locale);
};


const testAddon = require('./build/Release/cjoin.node');

module.exports.testAddon = testAddon;


// "defines": [
//     "NAPI_DISABLE_CPP_EXCEPTIONS"
// ]