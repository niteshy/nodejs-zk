"use strict";
var zookeeper = require('node-zookeeper-client');
var log = require("../../logger");
var zkPath = require('../lib/zkPath');
var _ = require('underscore');
var util = require('util');

module.exports = function (zclient, options) {
    var FNAME = " MEETING_DISCOVERY: ";
    var basePath = options.basePath;
    var meetings = null;
    var nsName = "event.service.addr";

    //var zklib = require('./zklib')(zclient);
    //zklib.watchAllChildren(basePath, {}, function(event) {
    //    log.info(FNAME, 'Got event', event);
    //}).then(function(m) {
    //    meetings = m;
    //});

    var addToMeeting = function (reqId, meeting, ip, port, cb) {
        // VS. Assert that meeting/ip/port are not null.
        var meetingAwareServicesPath = zkPath.join(basePath, nsName, meeting);
        var selfNode = ip + ":" + port;
        var meetingEventPath = zkPath.join(meetingAwareServicesPath, selfNode);
        log.info(reqId, FNAME, "Adding to the meeting path.", meetingEventPath);
        return zclient.mkdirp(meetingAwareServicesPath, new Buffer(selfNode), zookeeper.CreateMode.PERSISTENT,
                       function (err, created_path) {
                           if (err && err.name !== "NODE_EXISTS") {
                               log.error(reqId, FNAME, "failed to create meeting node: ",
                                   meetingAwareServicesPath, " due to: ", err);
                               return cb(err);
                           }
                           return zclient.create(meetingEventPath, new Buffer(JSON.stringify({
                                                   pid: process.pid,
                                                   url: util.format("http://%s:%s", ip, port)
                                                 })), zookeeper.CreateMode.EPHEMERAL,
                                                 function (err, evt_path) {
                                                     if (err && err.name !== "NODE_EXISTS") {
                                                         log.error(reqId, FNAME, "failed to create meeting node: ",
                                                             meetingEventPath, " due to: ", err);
                                                         return cb(err);
                                                     }
                                                     log.info(reqId, FNAME, "successfully created meeting node: ",
                                                         meetingEventPath);
                                                     return cb(null, meetingEventPath);
                                                 });
                       });
    };

    var removeFromMeeting = function (reqId, meeting, ip, port, cb) {
        if (! meeting || !ip || !port) {
            return (cb ? cb("INVALID_ARGUMENTS"): "INVALID_ARGUMENTS");
        }
        var meetingAwareServicesPath = zkPath.join(basePath, nsName, meeting);
        var selfNode = ip + ":" + port;
        var meetingEventPath = zkPath.join(meetingAwareServicesPath, selfNode);
        return zclient.remove(meetingEventPath, -1,
            function (err) {
                if (err && err.name !== "NO_NODE") {
                    log.error(reqId, FNAME, "failed to delete meeting node: ", meetingEventPath, " due to: ", err);
                    return cb(err);
                }
                return cb(null);
            });
    };

    // @deprecated
    function nodesForMeeting(meetingId) {
        //TODO: handle scenarios where meeting cache is empty
        if (! meetings || !meetings.children) {
            log.warn(FNAME + " MeetingDiscovery tree is empty. meetings = ", meetings);
            return null;
        }
        var meetingNodeMapping =  _.find(meetings.children, function (meeting) {
            return zkPath.childNode(meeting.path) === meetingId;
        });
        var meetingsInstances = meetingNodeMapping ? meetingNodeMapping.children : [];

        return _.map(meetingsInstances, function (x) {
            return JSON.parse(x.data).url;
        });
    }

    return {addNodeToMeeting: addToMeeting, nodesForMeeting: nodesForMeeting, removeNodeFromMeeting: removeFromMeeting };
};
