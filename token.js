var Q = require('q');

var jar;
var jarSaved;
var jarExpiration = 10 * 60 * 60 * 1000; // 10 hours

function getToken() {
    var dfd = Q.defer();
    if ( ! jar || jarSaved < (new Date()).getTime() - jarExpiration ) {
        require('./cookie')().then(function(newJar) {
            jar = newJar;
            jarSaved = (new Date()).getTime();
            dfd.resolve(jar);
        });
    } else {
        dfd.resolve(jar);
    }
    return dfd.promise;
};

module.exports = getToken;
