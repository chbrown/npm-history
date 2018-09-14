import * as http from 'http'
import * as request from 'request'
import * as moment from 'moment'
import {logger} from 'loge'
import {Connection} from 'sqlcmd-pg'

interface Package {
  id: number
  name: string
}

interface Statistic {
  package_id?: number
  day: Date
  downloads: number
}

export const db = new Connection({
  host: '127.0.0.1',
  port: 5432,
  database: 'npm-history',
})

// connect db log events to local logger
db.on('log', ev => {
  const args = [ev.format].concat(ev.args)
  logger[ev.level].apply(logger, args)
})

const NPM_EPOCH = new Date('2009-09-29')

/**
Custom-built 'INSERT INTO <table> (<columns>) VALUES (<row1>), (<row2>), ...;'
SQL query for inserting statistic rows. I don't think there are any limits on
the number of parameters you can have in a prepared query.
*/
function buildMultirowInsert(package_id: number, statistics: Statistic[]): [string, any[]] {
  const args: any[] = [package_id]
  const tuples: string[] = statistics.map(statistic => {
    const day_string = moment.utc(statistic.day).format('YYYY-MM-DD')
    const tuple = [
      '$1',
      `$${args.push(day_string)}`,
      `$${args.push(statistic.downloads)}`,
    ]
    return `(${tuple.join(', ')})`
  })
  return [`INSERT INTO statistic (package_id, day, downloads) VALUES ${tuples.join(', ')}`, args]
}

/**
Given a package name, return the full package row from the database, creating
one if needed.
*/
function findOrCreatePackage(name: string, callback: (error: Error, package_row?: Package) => void) {
  db.Select('package')
  .whereEqual({name})
  .execute((error: Error, rows: Package[]) => {
    if (error) return callback(error)
    if (rows.length > 0) return callback(null, rows[0])
    db.Insert('package')
    .set({name}).returning('*')
    .execute((insertError: Error, insertRows: Package[]) => {
      if (insertError) return callback(insertError)
      return callback(null, insertRows[0])
    })
  })
}

interface DownloadsRangeResponse {
  downloads: {day: string, downloads: number}[]
  start: string
  end: string
  package: string
  error?: string
}

/**
Given a package name (string) and start/end dates, call out to the NPM API
download-counts endpoint for that range. Collect these into a proper array of
Statistic objects, where `downloads` is zero for the days omitted from the response.
*/
function getRangeStatistics(name: string, start: moment.Moment, end: moment.Moment,
                            callback: (error: Error, statistics?: Statistic[]) => void) {
  const url = `https://api.npmjs.org/downloads/range/${start.format('YYYY-MM-DD')}:${end.format('YYYY-MM-DD')}/${name}`
  logger.debug('fetching "%s"', url)
  request.get({url, json: true}, (error: Error, response: http.IncomingMessage, body: DownloadsRangeResponse) => {
    if (error) return callback(error)

    let downloads_default = 0

    if (body.error) {
      // we consider missing stats (0008) a non-fatal error, though I'm not sure
      // what causes it. but instead of setting the downloads for those dates to 0,
      // which is probably what the server means, we set them to -1 (as a sort of error value)
      if (body.error == 'no stats for this package for this range (0008)') {
        body.downloads = []
        downloads_default = -1
      }
      else {
        return callback(new Error(body.error))
      }
    }

    logger.debug('retrieved %d counts', body.downloads.length)

    // body.downloads is a list of Statistics, but it's not very useful because
    // it might be missing dates.
    const downloads: {[day: string]: number} = {}
    body.downloads.forEach(download => downloads[download.day] = download.downloads)

    // The start/end in the response should be identical to the start/end we
    // sent. Should we check?
    const statistics: Statistic[] = []
    // Use start as a cursor for all the days we want to fill
    while (!start.isAfter(end)) {
      const day = start.format('YYYY-MM-DD')
      statistics.push({
        day: start.clone().toDate(),
        downloads: downloads[day] || downloads_default,
      })
      start.add(1, 'day')
    }

    callback(null, statistics)
  })
}

/**
Iterate through the given statistics, which should be sorted from earliest to
latest, and return the number of statistics at the beginning where the downloads
count is -1 (meaning, invalid).
*/
function countMissingStatistics(statistics: Statistic[]): number {
  let missing = 0
  for (const length = statistics.length; missing < length; missing++) {
    if (statistics[missing].downloads > -1) {
      break
    }
  }
  return missing
}

/**
Given a list of the statistics (pre-sorted from earliest to latest), return the
start and end dates (moments) of the next batch that we should request.

The returned endpoints should be inclusive, since the API takes inclusive
endpoint. I.e., we'll fetch the downloads for each of the bounding days as well
as all the days in between.

what we have:                           |now
               000000XXXXXXXXXXXXXXXXX??|        => fetch (case 1)
                                  ??????|        => fetch (case 1)
                    ??????XXXXXXXXXXXXXX|        => fetch (case 2)
               000000XXXXXXXXXXXXXXXXXXX|        => do nothing (case 3)

X = valid looking count
0 = invalid count
Each character represents 1 month or so.
*/
function determineNeededEndpoints(statistics: Statistic[],
                                  min_range_days: number,
                                  max_range_days: number): [moment.Moment, moment.Moment] {
  const latest_statistic = statistics[statistics.length - 1] || {day: NPM_EPOCH, downloads: -1}
  const latest = moment.utc(latest_statistic.day)
  // default to starting with the most recent period
  const now = moment.utc()
  // but NPM download counts are not available until after the day is over.
  now.subtract(1, 'day')
  // and they're updated shortly after the UTC day is over. To be safe, we'll
  // assume that 2015-05-20 counts are available as of 2015-05-21 at 6am (UTC),
  // so if it's currently before 6am, we'll go back another day to be safe.
  if (now.hour() < 6) {
    now.subtract(1, 'day')
  }
  // case 1) if there at least `min_range_days` missing since the latest
  // statistic we've fetched, we fetch from the latest to the current day
  // e.g.: today.diff(two_days_ago, 'days') => 2
  if (now.diff(latest, 'days') >= min_range_days) {
    // set the end point to `max_range_days` after the latest fetched statistic,
    // back, but don't reach into the future.
    const start = latest.clone().add(1, 'days')
    const end = moment.min(start.clone().add(max_range_days - 1, 'days'), now)
    return [start, end]
  }
  else {
    // otherwise, we fill in the backlog
    const backlog_exhausted = countMissingStatistics(statistics) > 180
    // case 3) if we've fetched more than half a year of invalid counts, we assume
    // that we've exhausted the available data going back, and won't ask for any more.
    if (backlog_exhausted) {
      return [null, null]
    }
    // case 2) set the endpoints to the most recent period preceding all of the
    // data we currently have.
    // if we've gotten to this point, statistics is sure to be non-empty
    const earliest = moment.utc(statistics[0].day)
    // we work backwards from the earliest date we do have, setting the end to
    // the day preceding the earliest day we've collected so far
    const end = earliest.subtract(1, 'day')
    const start = end.clone().subtract(max_range_days - 1, 'days')
    return [start, end]
  }
}

export function getPackageStatistics(name: string,
                                     min_range_days: number,
                                     max_range_days: number,
                                     callback: (error: Error, statistics?: Statistic[]) => void) {
  findOrCreatePackage(name, (error: Error, package_row: Package) => {
    if (error) return callback(error)

    // 1. find what dates we have so far
    db.Select('statistic')
    .add('day', 'downloads')
    .whereEqual({package_id: package_row.id})
    .orderBy('day')
    .execute((selectError: Error, local_statistics: Statistic[]) => {
      if (selectError) return callback(selectError)

      // 2. determine what we want to get next
      const [start, end] = determineNeededEndpoints(local_statistics, min_range_days, max_range_days)

      // 3. determineNeededEndpoints may return [null, null] if there are no
      // remaining ranges that we need to fetch
      if (start === null && end === null) {
        logger.debug('not fetching any data for "%s"', name)
        return callback(null, local_statistics)
      }

      // 4. get the next unseen statistics
      getRangeStatistics(name, start, end, (statsError, statistics) => {
        if (statsError) return callback(statsError)

        // 5. save the values we just fetched
        const [sql, args] = buildMultirowInsert(package_row.id, statistics)
        db.executeSQL(sql, args, (sqlError: Error) => {
          if (sqlError) return callback(sqlError)

          // 6. merge local and new statistics for the response
          const total_statistics = statistics.concat(local_statistics).sort((a, b) => a.day.getTime() - b.day.getTime())
          callback(null, total_statistics)
        })
      })
    })
  })
}

/**
`start` should precede `end`.
*/
export function queryAverageDownloads(start: moment.Moment,
                                      end: moment.Moment,
                                      callback: (error: Error, packages?: {[index: string]: number}) => void) {
  db.Select('package_statistic')
  .add('name', 'AVG(downloads)::int AS average')
  .where('downloads > -1')
  .where('day >= ?', start.toDate())
  .where('day < ?', end.toDate())
  .groupBy('name')
  .orderBy('name')
  .execute((error: Error, rows: {name: string, average: number}[]) => {
    if (error) return callback(error)
    logger.info('averaged downloads for %d packages', rows.length)
    const packages: {[index: string]: number} = {}
    rows.forEach(row => packages[row.name] = row.average)
    callback(null, packages)
  })
}
