'use strict';
var _ = require('underscore');
var fs = require('fs');
var request = require('request').defaults({ jar: true });
var Q = require('q');

var jar;
var jarSaved;
var jarExpiration = 10 * 60 * 60 * 1000; // 10 hours

var HttpFS = function(params) {
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

    function getCookie(options) {
        if ( ! options ) { options = {}; }
        var dfd = Q.defer();

        var port = 8443;
        var path = '/j_security_check';

        var url = 'https://' + server + ':' + port + path;

        var j = request.jar();

        if ( options.verbose ) { console.info('getting cookie'); }
        request.post({
            url: url,
            form: {
                j_username: params.user,
                j_password: params.password,
            },
            jar: j
        }, function (error) {
            if ( options.verbose ) { console.info('error in cookie response'); }
            if ( error ) {
                if ( options.verbose ) { console.info('error in cookie response'); }
                dfd.reject(error);
            } else {
                if ( options.verbose ) { console.info('cookie retreived'); }
                var cookie_string = j.getCookieString(url).split('; ');
                if ( cookie_string.length < 2 ) {
                    if ( options.verbose ) { console.info('cookie login failed'); }
                    // this indicates that login was a failure
                    dfd.reject({
                        message: 'Login failed',
                        exception: 'LoginFailed'
                    });
                } else {
                    if ( options.verbose ) { console.info('cookie success'); }
                    dfd.resolve(j);
                }
            }
        });

        return dfd.promise;
    }

    function getToken(options) {
        if ( ! options ) { options = {}; }
        var dfd = Q.defer();
        if ( ! jar || jarSaved < (new Date()).getTime() - jarExpiration ) {
            if ( options.verbose ) { console.info('getting jar'); }
            getCookie(options).then(function(newJar) {
                if ( options.verbose ) { console.info('jar got'); }
                jar = newJar;
                jarSaved = (new Date()).getTime();
                dfd.resolve(jar);
            }).fail(dfd.reject);
        } else {
            if ( options.verbose ) { console.info('using existing jar'); }
            dfd.resolve(jar);
        }
        return dfd.promise;
    }

    function makeRequest(query, options) {
        if ( ! options ) { options = {}; }
        var dfd = Q.defer();
        var port = 14443;
        var path = '/webhdfs/v1/';

        var url = 'https://' + server + ':' + port + path + query;

        function requestCallback(error, response, body) {
            if ( options.verbose ) { console.info('request callback'); }
            if ( error ) {
                if ( options.verbose ) { console.info('request error', error); }
                dfd.reject(error);
            } else {
                if ( options.verbose ) { console.info('request back', body); }
                // try and parse as JSON, but don't worry too
                // much about getting it right.
                try { body = JSON.parse(body);
                } catch(e) { }

                if ( body && body.RemoteException ) {
                    switch(body.RemoteException.exception) {
                        case 'AccessControlException':
                            var args = {};
                            var message = body.RemoteException.message;
                            var messageArgs = message.split('Permission denied:').pop();
                            messageArgs.split(',').map(function(arg) {
                                arg = arg.split('=');
                                if ( arg.length === 2 ) {
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
                            dfd.reject(body.RemoteException);
                        break;
                    }
                } else {
                    dfd.resolve(body);
                }
            }
        }

        if ( options.verbose ) { console.info('getting token'); }
        getToken(options).then(function(j) {
            var requestOptions = 
                _.extend({
                uri: url,
                jar: j,
                method: 'get'
            }, options); 

            if ( options.verbose ) { console.info('make request', requestOptions.uri); }

            request(requestOptions, requestCallback);
        }).fail(dfd.reject);

        return dfd.promise;
    }

    // Exposed functions

    function listDirectory(dir, options) {
        return makeRequest(dir+'?op=LISTSTATUS', options).then(function(results) {
            if ( results && results.FileStatuses && results.FileStatuses.FileStatus ) {
                return results.FileStatuses.FileStatus;
            }
        });
    }

    function createDirectory(dir, options) {
        if ( ! options ) { options = {}; }
        options.method = 'put';
        dir += '/';
        return makeRequest(dir+'?op=MKDIRS', options);
    }

    function upload(remoteFile, localFile) {
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
    }

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
    }

    function remove(path) {
        return makeRequest(path+'?op=DELETE', {
            method: 'DELETE'
        });
    }

    function move(source, destination, options) {
        if ( ! options ) { options = {}; }
        //curl -i -X PUT -b cookie.jar "http://host:port/webhdfs/v1/tmp/myDir?op=RENAME&destination=/tmp/yourDir"
        options.method = 'PUT';
        if ( destination.substring(0) !== '/' ) { destination = '/' + destination; }
        return makeRequest(source+'?op=RENAME&destination='+destination, options);
    }

    return {
        listDirectory: listDirectory,
        createDirectory: createDirectory,
        upload: upload,
        download: download,
        remove: remove,
        move: move
    };
};

module.exports = HttpFS;
