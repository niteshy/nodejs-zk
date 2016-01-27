/**
 * Created by nitesh on 27/01/16.
 */
var config  = require("../config");
var log     = require("../logger");
var zkConnection = require("../framework/src/zkConnection");

process.env.DENIM_ZOOKEEPER_HOSTS = config.get("DENIM_ZOOKEEPER_HOSTS");
process.title = "NODE-ZK";

var zkSetup = function(cb) {
    return zkConnection.init(function (err, zclient) {
        if (err) {
            log.error("Failed to initialize zookeeper");
            return cb(err);
        }
        return cb(null, zclient);
    });
};


zkSetup(function() {});