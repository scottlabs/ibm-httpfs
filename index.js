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
    function makeRequest(query, options) {
        var dfd = Q.defer();
        var port = 14443;
        var path = '/webhdfs/v1/';

        var url = 'https://' + server + ':' + port + path + query;

        getToken().then(function(j) {
                console.log('z');
            request(_.extend({
                uri: url,
                jar: j,
                method: 'get'
            }, options), function (error, response, body) {
                console.log('a');
                if ( error ) {
                    dfd.reject(error);
                } else {
                    console.log(response.req._header);
                    //console.log(response.headers._header);
                    // don't worry about this, not a big deal.
                    try { body = JSON.parse(body);
                    } catch(e) { }

                    if ( body['RemoteException'] ) {
                        dfd.reject(body);
                    } else {
                        dfd.resolve(body);
                    }
                }
            });
        });

        return dfd.promise;
    };

    // Exposed functions

    function listDirectory(dir) {
        return makeRequest(dir+'?op=LISTSTATUS');
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
            var file = fs.createReadStream(localFile);
            options.followAllRedirects = true;
            options.multipart = [
                {
                body: fs.createReadStream(localFile),
                'content-type' : 'application/octet-stream',
                'Content-Type' : 'application/octet-stream',
                }
            ];
            //options.multipart = {chunked: true, data: [
            //]};

            //options.formData = {
                //field: 'val',
                //attachments: [
                    //file
                //]
            //};
        } else {
            options.body = localFile;
        }

        console.log(remoteFile);
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
        upload: upload,
        download: download,
        remove: remove
    };
};

module.exports = HDFS;
