# Description

This exposes an API for interacting with IBM's HDFS (Hadoop Distributed File System). In particular, it exposes a simple request function upon which additional routes can be created.

I've only written functions for listing directories and uploading files. However, full documentation on IBM's HDFS [can be found here](http://www-01.ibm.com/support/knowledgecenter/SSPT3X_3.0.0/com.ibm.swg.im.infosphere.biginsights.admin.doc/doc/admin_fileupload_rest_apis.html) if you want to implement other routes.

# Install

    npm install ibm-hdfs

# Use

```
var HDFS = require('ibm-hdfs');

var hdfs = new HDFS({
    user: 'foo',
    password: 'bar',
    url: 'https://server:port/webhdfs/v1/'
});

console.log(hdfs.listDirectory('/dir'));
