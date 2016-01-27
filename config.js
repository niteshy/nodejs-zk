/**
 * Created by nitesh on 27/01/16.
 */

var conf = require('nconf').argv().env().file({ file: __dirname+'/config.json' });
module.exports = conf;
process.env.ZOOKEEPER_HOSTS = conf.get("ZOOKEEPER_HOSTS");
process.env.MY_NODE_IP = conf.get('MY_NODE_IP');
process.env.MY_PORT = conf.get("MY_PORT");