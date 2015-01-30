var request = require('request').defaults({ jar: true });
var Q = require('q');
var config = require('../config');
var getToken = require('./token');

function makeRequest(query) {
    var dfd = Q.defer();
    var port = 14443;
    var path = '/webhdfs/v1/';
    var host = 'bi-hadoop-prod-'+config.id+'.services.dal.bluemix.net';

    var url = 'https://' + host + ':' + port + path + query;

    getToken().then(function(j) {
        request.get({
            url: url,
            jar: j
        }, function (error, response, body) {
            //console.log(body);
            if ( error ) {
                dfd.reject(error);
            } else {
                dfd.resolve(body);
            }
        });
    });

    return dfd.promise;
};

module.exports = makeRequest;
