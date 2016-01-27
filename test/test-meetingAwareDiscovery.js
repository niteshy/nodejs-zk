/**
 * Created by nitesh on 1/27/16.
 */

var _ = require("underscore");
var config  = require("../config");
var log     = require("../logger");
var zkConnection = require("../framework/src/zkConnection");

process.title = "NODE-ZK";

var meetingAwareDiscovery;
var meetingAwarePath = "/bjn/test/seam/meeting.services";
var interval = config.get("EXECUTION:INTERVAL");
var perInterval = config.get("EXECUTION:PER_INTERVAL");
var nInterval = config.get("EXECUTION:N_INTERVAL");

var zkSetup = function(cb) {
    return zkConnection.init(function (err, zclient) {
        if (err) {
            log.error("zkSetup: Failed to initialize zookeeper");
            return cb(err);
        }
        return cb(null, zclient);
    });
};


var generateLoad = function(it) {
    var items = _.range(1, perInterval);
    _.each(items, function(id) {
        var reqId = it * (nInterval + id);
        var meetingId = reqId;
        meetingAwareDiscovery.addNodeToMeeting(reqId, meetingId, process.env.MY_NODE_IP, process.env.MY_PORT,
            function (err) {
                if (err) {
                    log.error("%s - ZK meetingAwareRegistry add failed for meeting = %s, endpointGuid = %s, err = %s",
                        reqId, meetingId, reqId, err);
                } else {
                    log.info("%s - successfully added to meetingAwareRegistry for meeting = %s, endpointGuid = %s",
                        reqId, meetingId, reqId);
                }
            }
        );

    });
};

zkSetup(function(err, zclient) {
    if (err) {
        return log.error("zkSetup: Could not able to connect zk, hence terminating test");
    } else {
        meetingAwareDiscovery = require('../framework/recipes/meetingAwareDiscovery')(zclient, {basePath: meetingAwarePath});
        var i = 1;
        setInterval(function() {
            generateLoad(i++);
        }, interval);
    }
});
