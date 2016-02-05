"use strict";
var zookeeper = require('node-zookeeper-client');
var log = require("../../logger");
var zkPath = require('../lib/zkPath');

var _ = require('underscore');
var util = require('util');

module.exports = function (zkWrapper, options) {
    var zklib = require('../src/zklib')(zkWrapper);

    var FNAME = " MEETING_DISCOVERY: ";
    var meetings = null;
    var basePath = options.basePath;
    var nsName = "event.service.addr";

    var createMeetingDiscoveryNode = function (reqId, meetingEventPath, ip, port, cb) {
        return zkWrapper.zclient.create(meetingEventPath, new Buffer(JSON.stringify({
                pid: process.pid,
                url: util.format("http://%s:%s", ip, port)
            })), zookeeper.CreateMode.EPHEMERAL,
            function(err) {
                if(err && err.name !== 'NODE_EXISTS') {
                    log.error(reqId, FNAME, "failed to create meeting discovery node: ", meetingEventPath, " due to: ", err);
                    return cb(err);
                }
                if (err && err.name === "NODE_EXISTS") {
                    log.info(reqId, FNAME, "Node %s exists already, deleting and recreating.", meetingEventPath);
                    return zkWrapper.zclient.remove(meetingEventPath, function  (err, res) {
                        if (err && err.name !== "NO_NODE") {
                            log.error(reqId, FNAME, "failed to remove meeting discovery node: ", meetingEventPath, " due" +
                                " to: ", err);
                            return cb(err);
                        }
                        return createMeetingDiscoveryNode(reqId, meetingEventPath, ip, port, cb);
                    });
                }
                log.info(reqId, FNAME, "successfully created meeting discovery node: ", meetingEventPath);
                return cb(null, meetingEventPath);
            });
    };

    var addToMeeting = function (reqId, meeting, ip, port, cb) {
        // VS. Assert that meeting/ip/port are not null.
        var meetingAwareServicesPath = zkPath.join(basePath, nsName, meeting);
        var selfNode = ip + ":" + port;
        var meetingEventPath = zkPath.join(meetingAwareServicesPath, selfNode);
        var nParticipants = null;
        //if ((nParticipants = cache.getParticipantsByMeetingId(meeting).length) > 0) {
        //    log.info(reqId, FNAME, " meeting tree cache already exist for meeting = ", meeting, " nParticipant = ", nParticipants);
        //    return cb(null, meetingEventPath);
        //}
        log.info(reqId, FNAME, "Adding to the meeting path.", meetingEventPath);
        return zkWrapper.zclient.mkdirp(meetingAwareServicesPath, new Buffer(selfNode), zookeeper.CreateMode.PERSISTENT,
            function (err, created_path) {
                if (err && err.name !== "NODE_EXISTS") {
                    log.error(reqId, FNAME, "failed to create meeting node: ", meetingAwareServicesPath, " due to: ", err);
                    return cb(err);
                }
                return createMeetingDiscoveryNode(reqId, meetingEventPath, ip, port, function (err, res) {
                    if (err) {
                        return cb(err);
                    }
                    // register the hook to add self meeting discovery node after session recovery
                    zkWrapper.on('SYNC_CONNECTED', function () {
                        //if (cache.getParticipantsByMeetingId(meeting).length > 0) {
                            log.warn(reqId, FNAME, "Zk connection was disconnected, but now that the connection  is restored, we are registering.");
                            createMeetingDiscoveryNode(reqId, meetingEventPath, ip, port, function (err, res) {
                                if (err) {
                                    return log.error(reqId, FNAME, "Failed to restore meeting discover for meeting ", meeting);
                                }
                                return log.info(reqId, FNAME, "successfully restored meeting discovery service on zk reconnect");
                            });
                        //} else {
                        //    return log.info(reqId, FNAME, " no participant present, hence not restoring meeting discovery service on zk reconnect");
                        //}
                    });
                    return cb(err, res);
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
        return zkWrapper.zclient.remove(meetingEventPath, -1,
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
