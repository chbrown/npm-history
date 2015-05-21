/// <reference path="type_declarations/index.d.ts" />
import async = require('async');
import path = require('path');
import http = require('http');
import moment = require('moment');
import request = require('request');
import yargs = require('yargs');

var sqlcmd = require('sqlcmd-pg');
var logger = require('loge');

interface Package {
  id: number;
  name: string;
}

interface Statistic {
  package_id?: number;
  day: string;
  downloads: number;
}

const db = new sqlcmd.Connection({
  host: '127.0.0.1',
  port: '5432',
  user: 'postgres',
  database: 'npm-history',
});

// connect db log events to local logger
db.on('log', function(ev) {
  var args = [ev.format].concat(ev.args);
  logger[ev.level].apply(logger, args);
});

const RANGE_DAYS = 180;
// const NPM_EPOCH = moment.utc('2009-09-29');

function buildMultirowInsert(package_id: number, statistics: Statistic[]) {
  var args: any[] = [package_id];
  var tuples: string[] = statistics.map(statistic => {
    var tuple = [
      '$1',
      '$' + args.push(statistic.day),
      '$' + args.push(statistic.downloads),
    ];
    return `(${tuple.join(', ')})`;
  });
  return [`INSERT INTO statistic (package_id, day, downloads) VALUES ${tuples.join(', ')}`, args];
}

function findOrCreatePackage(name: string, callback: (error: Error, package?: Package) => void) {
  db.Select('package')
  .whereEqual({name: name})
  .execute((error: Error, rows: Package[]) => {
    if (error) return callback(error);
    if (rows.length > 0) return callback(null, rows[0]);
    db.Insert('package')
    .set({name: name}).returning('*')
    .execute((error: Error, rows: Package[]) => {
      return callback(null, rows[0]);
    });
  });
}

function getPackageStatistics(name: string, callback: (error: Error, statistics?: Statistic[]) => void) {
  findOrCreatePackage(name, (error: Error, package: Package) => {
    if (error) return callback(error);

    // 1. find what dates we have so far
    db.Select('statistic')
    .add('day', 'downloads')
    .whereEqual({package_id: package.id})
    .orderBy('day')
    .execute((error: Error, local_statistics: Statistic[]) => {
      if (error) return callback(error);
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
      var url = `https://api.npmjs.org/downloads/range/${start.format('YYYY-MM-DD')}:${end.format('YYYY-MM-DD')}/${name}`;
      logger.debug('requesting url: %s', url);
      request.get({url: url, json: true}, (error, response, body) => {
        if (error) return callback(error);

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
        var downloads: {[day: string]: number} = {};
        body.downloads.forEach((statistic: Statistic) => { downloads[statistic.day] = statistic.downloads });

        logger.debug('retrieved %d counts', body.downloads.length);

        // the start/end in the response should be identical to the start/end we sent
        var statistics: Statistic[] = [];
        // use start as a cursor for all the days we want to fill
        while (!start.isAfter(end)) {
          var day = start.format('YYYY-MM-DD');
          statistics.push({day: day, downloads: downloads[day] || downloads_default});
          start.add(1, 'day');
        }

        // 5. save the values we just fetched
        var [sql, args] = buildMultirowInsert(package.id, statistics);
        db.executeSQL(sql, args, (error: Error) => {
          if (error) return callback(error);
          // merge local and new statistics
          Array.prototype.unshift.apply(local_statistics, statistics);
          callback(null, local_statistics);
        });
      });
    });
  });
}

class Controller {
  server: http.Server;
  constructor() {
    this.server = http.createServer((req, res) => {
      logger.debug('%s %s', req.method, req.url);
      var package_downloads_match = req.url.match(/^\/packages\/(.+)\/downloads$/);
      if (package_downloads_match) {
        getPackageStatistics(package_downloads_match[1], (error: Error, statistics?: Statistic[]) => {
          if (error) {
            res.statusCode = 500;
            return res.end(`getPackageStatistics error: ${error.message}`);
          }
          res.setHeader('Content-Type', 'application/json');
          if (req.method == 'HEAD') {
            return res.end('');
          }
          var result = statistics.map(statistic => {
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

    this.server.on('listening', () => logger.info('server listening'));
  }
  listen(port: number, hostname?: string) {
    this.server.listen(port, hostname);
  }
}

export function cli() {
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
    db.createDatabaseIfNotExists(error => {
      if (error) throw error;
      db.executePatches('_migrations', __dirname, error => {
        if (error) throw error;
        new Controller().listen(argv.port, argv.hostname);
      });
    });
  }
}
