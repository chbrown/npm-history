var url = require('url');
var moment = require('moment');
var regex_router_1 = require('regex-router');
var database_1 = require('./database');
var package_json = require('./package.json');
var R = new regex_router_1.default();
/**
HEAD|GET /packages/:name/downloads

Retrieve another period's worth of downloads for the package named `name`.
If `name` is empty, requests global counts.

The period defaults to 180 days for individual packages,
or 10 days for global counts.

Returns all saved downloads for that package as JSON, unless called with HEAD.
*/
R.any(/^\/packages\/(.*)\/downloads$/, function (req, res, match) {
    var name = match[1];
    // take it easy on the NPM API server for all-packages requests
    var _a = (name == '') ? [5, 10] : [30, 180], min_range_days = _a[0], max_range_days = _a[1];
    database_1.getPackageStatistics(name, min_range_days, max_range_days, function (error, statistics) {
        if (error) {
            res.statusCode = 500;
            return res.end("error getting package statistics: " + error.message);
        }
        res.setHeader('Content-Type', 'application/json');
        if (req.method == 'HEAD') {
            return res.end();
        }
        var result = statistics.map(function (statistic) {
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
R.get(/^\/packages\/averages/, function (req, res) {
    var query = url.parse(req.url, true).query;
    var end = query.end ? moment(query.end) : moment();
    var start = query.start ? moment(query.start) : end.clone().subtract(60, 'days');
    database_1.queryAverageDownloads(start, end, function (error, packages) {
        if (error) {
            res.statusCode = 500;
            return res.end("error getting package averages: " + error.message);
        }
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify(packages) + '\n');
    });
});
/** GET /info
Show npm-history package metadata
*/
R.get(/^\/info$/, function (req, res, m) {
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
exports.controller = R;
