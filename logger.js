var winston = require('winston');
var mkdirp = require('mkdirp');
var util = require('util');

var log_dir = "./logs/";
mkdirp.sync(log_dir);
console.log("Setting up the logs on ", log_dir);

var log = new(winston.Logger)();
if (process.env.LOG_CONSOLE === "true") {
    log.add(winston.transports.Console);
} else {
    log.add(winston.transports.DailyRotateFile, {
        filename: util.format('nodejs-zk.%s.', process.env.PORT || 8000),
        dirname: log_dir,
        json: false,
        maxsize: 102400000, /* 100 mb */
        level: 'debug',
        datePattern: '.yyyy-MM-dd.log',
        timestamp: function(){
            return getFormattedDate();
            function getFormattedDate() {
                var temp = new Date();

                return padStr(temp.getFullYear()) + "-" +
                    padStr(1 + temp.getMonth()) + "-" +
                    padStr(temp.getDate()) + " " +
                    padStr(temp.getHours()) + ":" +
                    padStr(temp.getMinutes()) + ":" +
                    padStr(temp.getSeconds()) + "," +
                    padStr(temp.getMilliseconds());
            }
            function padStr(i) {
                return (i < 10) ? "0" + i : "" + i;
            }
        }
    });
}

process.on('uncaughtException', function(err){
    log.error("Unhandled exception occurred : " , err.stack || err);
});

module.exports = log;
module.exports.stream = {
    write: function(message, encoding){
        log.info(message);
    }
};
