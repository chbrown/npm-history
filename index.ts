import * as path from 'path';
import * as http from 'http';
import * as yargs from 'yargs';
import {logger, Level} from 'loge';

import {db} from './database';
import {controller} from './controller';

var server = http.createServer((req, res) => {
  logger.debug('%s %s', req.method, req.url);
  // enable CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', '*');
  controller.route(req, res);
});
server.on('listening', () => {
  var address = server.address();
  logger.info(`server listening on http://${address.address}:${address.port}`);
});
server['timeout'] = 10*60*1000; // defaults to 2 * 60 * 1000 = 120000 (2 minutes)

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
      port: parseInt(process.env.PORT, 10) || 8080,
    })
    .boolean(['help', 'verbose', 'version']);

  var argv = argvparser.argv;
  logger.level = argv.verbose ? Level.debug : Level.info;

  if (argv.help) {
    yargs.showHelp();
  }
  else if (argv.version) {
    console.log(require('./package').version);
  }
  else {
    db.createDatabaseIfNotExists(error => {
      if (error) throw error;

      db.executePatches('_migrations', path.join(__dirname, 'migrations'), error => {
        if (error) throw error;

        server.listen(argv.port, argv.hostname);
      });
    });
  }
}
