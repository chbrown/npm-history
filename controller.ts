/// <reference path="type_declarations/index.d.ts" />
import url = require('url');
import Router = require('regex-router');

import {getPackageStatistics, queryAverageDownloads} from './database';

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

export var controller = R;