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

const NPM_EPOCH = '2009-09-29';

/**
Custom-built 'INSERT INTO <table> (<columns>) VALUES (<row1>), (<row2>), ...;'
SQL query for inserting statistic rows. I don't think there are any limits on
the number of parameters you can have in a prepared query.
*/
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

/**
Given a package name, return the full package row from the database, creating
one if needed.
*/
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

/**
Given a package name (string) and start/end dates, call out to the NPM API
download-counts endpoint for that range. Collect these into a proper array of
Statistic objects, where `downloads` is zero for the days omitted from the response.
*/
function getRangeStatistics(name: string, start: moment.Moment, end: moment.Moment,
                            callback: (error: Error, statistics?: Statistic[]) => void) {
  var url = `https://api.npmjs.org/downloads/range/${start.format('YYYY-MM-DD')}:${end.format('YYYY-MM-DD')}/${name}`;
  logger.debug('fetching "%s"', url);
  request.get({url: url, json: true}, (error: Error, response: http.IncomingMessage, body: any) => {
    if (error) return callback(error);

    var downloads_default = 0;

    if (body.error) {
      // we consider missing stats (0008) a non-fatal error, though I'm not sure
      // what causes it. but instead of setting the downloads for those dates to 0,
      // which is probably what the server means, we set them to -1 (as a sort of error value)
      if (body.error == "no stats for this package for this range (0008)") {
        body.downloads = [];
        downloads_default = -1;
      }
      else {
        return callback(new Error(body.error));
      }
    }

    logger.debug('retrieved %d counts', body.downloads.length);

    // body.downloads is a list of Statistics, but it's not very useful because
    // it might be missing dates.
    var downloads: {[day: string]: number} = {};
    body.downloads.forEach((statistic: Statistic) => { downloads[statistic.day] = statistic.downloads });

    // The start/end in the response should be identical to the start/end we
    // sent. Should we check?
    var statistics: Statistic[] = [];
    // Use start as a cursor for all the days we want to fill
    while (!start.isAfter(end)) {
      var day = start.format('YYYY-MM-DD');
      statistics.push({day: day, downloads: downloads[day] || downloads_default});
      start.add(1, 'day');
    }

    callback(null, statistics);
  });
}

/**
Given a list of the statistics (pre-sorted from earliest to latest), return the
start and end dates (moments) of the next batch that we should request.
*/
function determineNeededEndpoints(statistics: Statistic[], range_days: number): [moment.Moment, moment.Moment] {
  // default to starting with the most recent period
  var end = moment.utc();
  // but NPM download counts are not available until after the day is over.
  end.subtract(1, 'day');
  // they're updated shortly after the UTC day is over. To be safe, we'll
  // assume that 2015-05-20 counts are available as of 2015-05-21 at 6am (UTC),
  // so if it's currently before 6am, we'll go back another day.
  if (end.hour() < 6) {
    end.subtract(1, 'day');
  }
  // set the start point to however many days back
  var start = end.clone().subtract(range_days, 'days');
  // but if getting the most recent batch overlaps with what we already have...
  var latest = statistics[statistics.length - 1] || {day: NPM_EPOCH, downloads: -1};
  if (start.isBefore(moment.utc(latest.day))) {
    // ...then set the endpoints to the most recent period preceding all of the
    // data we currently have.
    // if we're in this conditional, statistics is sure to be non-empty
    var earliest = statistics[0];
    // and we work backwards from the earliest date we do have
    end = moment.utc(earliest.day).subtract(1, 'day');
    start = end.clone().subtract(range_days, 'days');
  }
  // TODO: fill in the gaps somehow?
  return [start, end];
}

/**
Iterate through the given statistics, which should be sorted from earliest to
latest, and return the number of statistics at the beginning where the downloads
count is -1 (meaning, invalid).
*/
function countMissingStatistics(statistics: Statistic[]): number {
  var missing = 0;
  for (var length = statistics.length; missing < length; missing++) {
    if (statistics[missing].downloads > -1) {
      break;
    }
  }
  return missing;
}

function getPackageStatistics(name: string, range_days: number,
                              callback: (error: Error, statistics?: Statistic[]) => void) {
  findOrCreatePackage(name, (error: Error, package: Package) => {
    if (error) return callback(error);

    // 1. find what dates we have so far
    db.Select('statistic')
    .add('day', 'downloads')
    .whereEqual({package_id: package.id})
    .orderBy('day')
    .execute((error: Error, local_statistics: Statistic[]) => {
      if (error) return callback(error);

      // 2. determine whether we seem to have gathered all stats for the package
      var missing = countMissingStatistics(local_statistics);
      // if we've fetched more than half a year of invalid counts, we assume
      // that we've exhausted the available data, and don't ask for any more.
      if (missing > 180) {
        logger.debug('not fetching any more data for "%s"', name);
        return callback(null, local_statistics);
      }

      // 3. determine what we want to get next
      var [start, end] = determineNeededEndpoints(local_statistics, range_days);

      // 4. get the next unseen statistics
      getRangeStatistics(name, start, end, (error, statistics) => {
        if (error) return callback(error);

        // 5. save the values we just fetched
        var [sql, args] = buildMultirowInsert(package.id, statistics);
        db.executeSQL(sql, args, (error: Error) => {
          if (error) return callback(error);

          // 6. merge local and new statistics for the response
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
      var package_downloads_match = req.url.match(/^\/packages\/(.*)\/downloads$/);
      if (package_downloads_match) {
        var name = package_downloads_match[1];
        // take it easy on the NPM API server for all-packages requests
        var range_days = (name == '') ? 10 : 180;
        getPackageStatistics(name, range_days, (error, statistics) => {
          if (error) {
            res.statusCode = 500;
            return res.end(`error getting package statistics: ${error.message}`);
          }
          res.setHeader('Content-Type', 'application/json');
          if (req.method == 'HEAD') {
            return res.end();
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
        res.end('Not found\n');
      }
    });

    this.server.on('listening', () => logger.info('server listening'));
  }
  listen(port: number, hostname?: string) {
    this.server.listen(port, hostname);
  }
}

export function main() {
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