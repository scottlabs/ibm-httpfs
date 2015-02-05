var _ = require('underscore');
var fs = require('fs');
var request = require('request').defaults({ jar: true });
var Q = require('q');

var jar;
var jarSaved;
var jarExpiration = 10 * 60 * 60 * 1000; // 10 hours

var HDFS = function(params) {
    if ( ! params ) { throw new Error("You must provide params."); }
    if ( ! params.user ) { throw new Error("You must provide a user."); }
    if ( ! params.password ) { throw new Error("You must provide a password."); }
    if ( ! params.url ) { throw new Error("You must provide a URL."); }

    // parse incoming URL, we'll need the server.
    var url = params.url.split('://');
    if ( url.length < 2 ) { throw "You must provide a valid URL."; }
    url = url.pop().split(':');
    if ( url.length < 2 ) { throw "You must provide a valid URL."; }
    var server = url.shift();

    function getCookie() {
        var dfd = Q.defer();

        var port = 8443;
        var path = '/j_security_check';

        var url = 'https://' + server + ':' + port + path;

        var j = request.jar();

        request.post({
            url: url,
            form: {
                j_username: params.user,
                j_password: params.password,
            },
            jar: j
        }, function (error, response, body) {
            if ( error ) {
                dfd.reject(error);
            } else {
                var cookie_string = j.getCookieString(url).split('; ');
                if ( cookie_string.length < 2 ) {
                    // this indicates that login was a failure
                    dfd.reject({
                        message: 'Login failed',
                        exception: 'LoginFailed'
                    });
                } else {
                    dfd.resolve(j);
                }
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
            }).fail(dfd.reject);
        } else {
            dfd.resolve(jar);
        }
        return dfd.promise;
    };

    function makeRequest(query, options) {
        var dfd = Q.defer();
        var port = 14443;
        var path = '/webhdfs/v1/';

        var url = 'https://' + server + ':' + port + path + query;
        console.log(url);

        function requestCallback(error, response, body) {
            console.log('r1');
            if ( error ) {
                dfd.reject(error);
            } else {
                // try and parse as JSON, but don't worry too
                // much about getting it right.
                try { body = JSON.parse(body);
                } catch(e) { }
                console.log('r2');

                if ( body && body['RemoteException'] ) {
                    console.log('exception1');
                    switch(body.RemoteException.exception) {
                        case 'AccessControlException':
                            console.log('acc');
                            var args = {};
                            var message = body.RemoteException.message;
                            var messageArgs = message.split('Permission denied:').pop();
                            messageArgs.split(',').map(function(arg) {
                                arg = arg.split('=');
                                if ( arg.length == 2 ) {
                                    args[arg[0].trim()] = arg[1].trim();
                                }
                            });
                            dfd.reject({
                                message: message,
                                javaClassName: body.RemoteException.javaClassName,
                                exception: body.RemoteException.exception,
                                args: args 
                            });
                        break;
                        default:
                            console.log('default');
                            dfd.reject(body.RemoteException);
                        break;
                    }
                } else {
                    console.log('body');
                    dfd.resolve(body);
                }
            }
        };

        console.log('a');
        getToken().then(function(j) {
            console.log('b');
            var requestOptions = 
                _.extend({
                uri: url,
                jar: j,
                method: 'get'
            }, options); 

            console.log(requestOptions);

            request(requestOptions, requestCallback);
        }).fail(dfd.reject);

        return dfd.promise;
    };

    // Exposed functions

    function listDirectory(dir) {
        return makeRequest(dir+'?op=LISTSTATUS').then(function(results) {
            if ( results && results.FileStatuses && results.FileStatuses.FileStatus ) {
                return results.FileStatuses.FileStatus;
            }
        });
    };

    function createDirectory(dir) {
        var options = {
            method: 'put',
        };
        dir += '/';
        return makeRequest(dir+'?op=MKDIR', options);
    };

    function upload(remoteFile, localFile) {
        var contents;

        var options = {
            method: 'put',
            headers: {
                'Content-Type': 'application/octet-stream',
                'Transfer-Encoding': 'chunked'
            }
        };
        if ( fs.existsSync(localFile) ) {
            options.file = localFile;
        } else {
            options.body = localFile;
        }

        return makeRequest(remoteFile + '?op=CREATE&data=true', options);
    };

    function download(path, local) {
        if ( local ) {
            var dfd = Q.defer();
            makeRequest(path+'?op=OPEN').then(function(response) {
                fs.writeFile(local, response, function(err) {
                    if ( err ) { dfd.reject(err); }
                    else { dfd.resolve(response); }
                });
            }).fail(dfd.reject);
            return dfd.promise;
        } else {
            return makeRequest(path+'?op=OPEN');
        }
    };

    function remove(path) {
        return makeRequest(path+'?op=DELETE', {
            method: 'DELETE'
        });
    };

    return {
        listDirectory: listDirectory,
        createDirectory: createDirectory,
        upload: upload,
        download: download,
        remove: remove
    };
};

module.exports = HDFS;
