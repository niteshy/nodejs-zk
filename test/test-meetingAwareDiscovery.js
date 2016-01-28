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
var interval    = config.get("EXECUTION:INTERVAL");
var perInterval = config.get("EXECUTION:PER_INTERVAL");
var nInterval   = config.get("EXECUTION:N_INTERVAL");

var zkSetup = function(cb) {
    return zkConnection.init(function (err, zclient) {
        if (err) {
            log.error("zkSetup: Failed to initialize zookeeper");
            return cb(err);
        }
        return cb(null, zclient);
    });
};

var stats = {};
stats.nitems = 0;
stats.success = [];
stats.failed = [];

var generateLoad = function(it, cb) {
    var items = _.range(1, perInterval+1);

    _.each(items, function(item, i) {
        var reqId = it * (1000000 + item);
        var meetingId = reqId;
        stats.nitems++;
        meetingAwareDiscovery.addNodeToMeeting(reqId, meetingId, process.env.MY_NODE_IP, process.env.MY_PORT,
            function (err) {
                if (err) {
                    log.error("%s - ZK meetingAwareRegistry add failed for meeting = %s, endpointGuid = %s, err = %s",
                        reqId, meetingId, reqId, err);
                    return stats.failed.push({"reqId": reqId, "meetingId": meetingId});
                } else {
                    log.info("%s - successfully added to meetingAwareRegistry for meeting = %s, endpointGuid = %s",
                        reqId, meetingId, reqId);
                    return stats.success.push({"reqId": reqId, "meetingId": meetingId});
                }
            }
        );
        if (i === (items.length - 1)) {
            return _.isFunction(cb) ? cb(null): null;
        }
    });
};

var printTestLoad = function () {
    var str = "\n*************** Generating load *************\n";
    str += "\t\t\t\t Interval = " +  interval + "(ms)\n";
    str += "\t\t\t\t No. of interval = " + nInterval + "\n";
    str += "\t\t\t\t No. of ops per interval = " + perInterval + "\n";
    str += "\t\t\t\t Total execution time taken = " + (nInterval * interval)/1000 + "(sec)\n";
    str += "\t\t\t\t *********************************************";
    log.info(str);
};

var printStats = function () {
    var str = "#################### Stats ####################\n";
    str += "\t\t\t\t Total items = " +  stats.nitems + "\n";
    str += "\t\t\t\t No. of success = " + stats.success.length + "\n";
    str += "\t\t\t\t No. of failure = " + stats.failed.length + "\n";
    str += "\t\t\t\t *********************************************\n";
    log.info(str);
};

zkSetup(function(err, zclient) {
    if (err) {
        return log.error("zkSetup: Could not able to connect zk, hence terminating test");
    } else {
        meetingAwareDiscovery = require('../framework/recipes/meetingAwareDiscovery')(zclient, {basePath: meetingAwarePath});
        var i = 1;
        printTestLoad();
        var timer = setInterval(function() {
            generateLoad(i, function () {
                i++;
                if (i === nInterval+1) {
                    clearInterval(timer);
                    setTimeout(printStats, 5000);
                }
            });
        }, interval);
    }
});
