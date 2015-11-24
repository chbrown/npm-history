import * as url from 'url';
import * as moment from 'moment';
import Router from 'regex-router';

import {getPackageStatistics, queryAverageDownloads} from './database';

var package_json = require('./package.json');

var R = new Router();

/**
HEAD|GET /packages/:name/downloads

Retrieve another period's worth of downloads for the package named `name`.
If `name` is empty, requests global counts.

The period defaults to 180 days for individual packages,
or 10 days for global counts.

Returns all saved downloads for that package as JSON, unless called with HEAD.
*/
R.any(/^\/packages\/(.*)\/downloads$/, (req, res, match) => {
  var name = match[1];
  // take it easy on the NPM API server for all-packages requests
  var [min_range_days, max_range_days] = (name == '') ? [5, 10] : [30, 180];
  getPackageStatistics(name, min_range_days, max_range_days, (error, statistics) => {
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
});

/**
GET /packages/averages?start=<date>&end=<date>

`end` defaults to the current date.
`start` defaults to 60 days before the end date.

This is a disk access/computationally expensive operation.
It can take several minutes, depending on the size of the period.
*/
R.get(/^\/packages\/averages/, (req, res) => {
  var query = url.parse(req.url, true).query;
  var end = query.end ? moment(query.end) : moment();
  var start = query.start ? moment(query.start) : end.clone().subtract(60, 'days');
  queryAverageDownloads(start, end, (error, packages) => {
    if (error) {
      res.statusCode = 500;
      return res.end(`error getting package averages: ${error.message}`);
    }
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify(packages) + '\n');
  });
});

/** GET /info
Show npm-history package metadata
*/
R.get(/^\/info$/, (req, res, m) => {
  var info = {
    name: package_json.name,
    version: package_json.version,
    description: package_json.description,
    homepage: package_json.homepage,
    author: package_json.author,
    license: package_json.license,
  };
  res.setHeader('Content-Type', 'application/json');
  res.end(JSON.stringify(info) + '\n');
});

export var controller = R;
