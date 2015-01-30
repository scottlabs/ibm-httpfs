var request = require('request').defaults({ jar: true });
var Q = require('q');

var jar;
var jarSaved;
var jarExpiration = 10 * 60 * 60 * 1000; // 10 hours

var HDFS = function(params) {
    // parse incoming URL, we'll need the server.
    var server = params.url.split('://').pop().split(':').shift();

    function getCookie() {
        var dfd = Q.defer();

        var port = 8443;
        var path = '/j_security_check';

        var url = 'https://' + server + ':' + port + path;

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

    function getToken() {
        var dfd = Q.defer();
        if ( ! jar || jarSaved < (new Date()).getTime() - jarExpiration ) {
            getCookie().then(function(newJar) {
                jar = newJar;
                jarSaved = (new Date()).getTime();
                dfd.resolve(jar);
            });
        } else {
            dfd.resolve(jar);
        }
        return dfd.promise;
    };
    function makeRequest(query) {
        var dfd = Q.defer();
        var port = 14443;
        var path = '/webhdfs/v1/';

        var url = 'https://' + server + ':' + port + path + query;

        getToken().then(function(j) {
            request.get({
                url: url,
                jar: j
            }, function (error, response, body) {
                if ( error ) {
                    dfd.reject(error);
                } else {
                    dfd.resolve(body);
                }
            });
        });

        return dfd.promise;
    };

    // Exposed functions

    function listDirectory(dir) {
        return makeRequest(dir+'?op=LISTSTATUS');
    };

    function upload(file) {
        return makeRequest('tmp/filename?op=CREATE', {
            type: 'put'
        });
    };

    return {
        listDirectory: listDirectory,
        upload: upload
    };
}();

module.exports = HDFS;
