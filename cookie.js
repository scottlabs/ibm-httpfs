var config = require('../config');
var request = require('request').defaults({ jar: true });
var Q = require('q');

function getCookie() {
    var dfd = Q.defer();

    var port = 8443;
    var path = '/j_security_check';
    var host = 'bi-hadoop-prod-'+config.id+'.services.dal.bluemix.net';

    var url = 'https://' + host + ':' + port + path;

    var j = request.jar();

    request.post({
        url: url,
        form: {
            j_username: config.user,
            j_password: config.password,
        },
        jar: j
    }, function (error, response, body) {
        if ( error ) {
            dfd.reject(error);
        } else {
            dfd.resolve(j);
        }
    });

    return dfd.promise;
};

module.exports = getCookie;
