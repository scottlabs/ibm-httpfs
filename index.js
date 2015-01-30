var makeRequest = require('./makeRequest');

var WebHDFS = function(params) {
    //user, password, url

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

module.exports = WebHDFS;
