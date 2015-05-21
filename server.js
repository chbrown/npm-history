var http = require('http');
var moment = require('moment');
var request = require('request');
var yargs = require('yargs');
var sqlcmd = require('sqlcmd-pg');
var logger = require('loge');
var db = new sqlcmd.Connection({
    host: '127.0.0.1',
    port: '5432',
    user: 'postgres',
    database: 'npm-history',
});
// connect db log events to local logger
db.on('log', function (ev) {
    var args = [ev.format].concat(ev.args);
    logger[ev.level].apply(logger, args);
});
var RANGE_DAYS = 180;
// const NPM_EPOCH = moment.utc('2009-09-29');
function buildMultirowInsert(package_id, statistics) {
    var args = [package_id];
    var tuples = statistics.map(function (statistic) {
        var tuple = [
            '$1',
            '$' + args.push(statistic.day),
            '$' + args.push(statistic.downloads),
        ];
        return "(" + tuple.join(', ') + ")";
    });
    return [("INSERT INTO statistic (package_id, day, downloads) VALUES " + tuples.join(', ')), args];
}
function findOrCreatePackage(name, callback) {
    db.Select('package')
        .whereEqual({ name: name })
        .execute(function (error, rows) {
        if (error)
            return callback(error);
        if (rows.length > 0)
            return callback(null, rows[0]);
        db.Insert('package')
            .set({ name: name }).returning('*')
            .execute(function (error, rows) {
            return callback(null, rows[0]);
        });
    });
}
function getPackageStatistics(name, callback) {
    findOrCreatePackage(name, function (error, package) {
        if (error)
            return callback(error);
        // 1. find what dates we have so far
        db.Select('statistic')
            .add('day', 'downloads')
            .whereEqual({ package_id: package.id })
            .orderBy('day')
            .execute(function (error, local_statistics) {
            if (error)
                return callback(error);
            // 2. determine what we want to get
            var local_earliest = local_statistics[0];
            var local_latest = local_statistics[local_statistics.length - 1];
            // NPM download counts are not available until after the day is over.
            var remote_latest = moment.utc().subtract(1, 'day');
            // and they're updated shortly after the UTC day is over. To be safe,
            // we'll assume that 2015-05-20 counts are available as of 2015-05-21 at
            // 6am (UTC), so if it's before 6am, we'll go back another day
            if (remote_latest.hour() < 6) {
                remote_latest.subtract(1, 'day');
            }
            var end;
            if (local_latest === undefined || remote_latest.diff(moment.utc(local_latest.day), 'days') > RANGE_DAYS) {
                // 3a. if we don't have any of the most recent 31 days, start with those
                end = remote_latest;
            }
            else {
                // 3b. otherwise, work backwards from the earliest date we do have
                end = moment.utc(local_earliest.day).subtract(1, 'day');
            }
            var start = end.clone().subtract(RANGE_DAYS, 'days');
            // TODO: fill in the gaps somehow
            // 4. get the next unseen statistics
            var url = "https://api.npmjs.org/downloads/range/" + start.format('YYYY-MM-DD') + ":" + end.format('YYYY-MM-DD') + "/" + name;
            logger.debug('requesting url: %s', url);
            request.get({ url: url, json: true }, function (error, response, body) {
                if (error)
                    return callback(error);
                var downloads_default = 0;
                if (body.error) {
                    // we'll consider "no stats for this package for this range (0008)" a
                    // non-fatal error, so that we can keep going
                    if (body.error == "no stats for this package for this range (0008)") {
                        body.downloads = [];
                        downloads_default = -1;
                    }
                    else {
                        return callback(new Error(body.error));
                    }
                }
                // body.downloads is a list of Statistics, but not very useful because
                // it might be missing dates.
                var downloads = {};
                body.downloads.forEach(function (statistic) { downloads[statistic.day] = statistic.downloads; });
                logger.debug('retrieved %d counts', body.downloads.length);
                // the start/end in the response should be identical to the start/end we sent
                var statistics = [];
                // use start as a cursor for all the days we want to fill
                while (!start.isAfter(end)) {
                    var day = start.format('YYYY-MM-DD');
                    statistics.push({ day: day, downloads: downloads[day] || downloads_default });
                    start.add(1, 'day');
                }
                // 5. save the values we just fetched
                var _a = buildMultirowInsert(package.id, statistics), sql = _a[0], args = _a[1];
                db.executeSQL(sql, args, function (error) {
                    if (error)
                        return callback(error);
                    // merge local and new statistics
                    Array.prototype.unshift.apply(local_statistics, statistics);
                    callback(null, local_statistics);
                });
            });
        });
    });
}
var Controller = (function () {
    function Controller() {
        this.server = http.createServer(function (req, res) {
            logger.debug('%s %s', req.method, req.url);
            var package_downloads_match = req.url.match(/^\/packages\/(.+)\/downloads$/);
            if (package_downloads_match) {
                getPackageStatistics(package_downloads_match[1], function (error, statistics) {
                    if (error) {
                        res.statusCode = 500;
                        return res.end("getPackageStatistics error: " + error.message);
                    }
                    var result = statistics.map(function (statistic) {
                        return {
                            day: moment.utc(statistic.day).format('YYYY-MM-DD'),
                            downloads: statistic.downloads,
                        };
                    });
                    res.end(JSON.stringify(result) + '\n');
                });
            }
            else {
                res.statusCode = 404;
                res.end('Not found');
            }
        });
        this.server.on('listening', function () { return logger.info('server listening'); });
    }
    Controller.prototype.listen = function (port, hostname) {
        this.server.listen(port, hostname);
    };
    return Controller;
})();
function cli() {
    var argvparser = yargs
        .usage('Usage: npm-history -p 80')
        .describe({
        hostname: 'hostname to listen on',
        port: 'port to listen on',
        help: 'print this help message',
        verbose: 'print extra output',
        version: 'print version',
    })
        .alias({
        h: 'help',
        p: 'port',
        v: 'verbose',
    })
        .default({
        hostname: process.env.HOSTNAME || '127.0.0.1',
        port: process.env.PORT || 8080,
    })
        .boolean(['help', 'verbose', 'version']);
    var argv = argvparser.argv;
    logger.level = argv.verbose ? 'debug' : 'info';
    if (argv.help) {
        yargs.showHelp();
    }
    else if (argv.version) {
        console.log(require('./package').version);
    }
    else {
        db.createDatabaseIfNotExists(function (error) {
            if (error)
                throw error;
            db.executePatches('_migrations', __dirname, function (error) {
                if (error)
                    throw error;
                new Controller().listen(argv.port, argv.hostname);
            });
        });
    }
}
exports.cli = cli;
