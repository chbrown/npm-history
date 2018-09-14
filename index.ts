import * as path from 'path';
import * as http from 'http';
import * as optimist from 'optimist';
import {logger, Level} from 'loge';
import {executePatches} from 'sql-patch';

import {db} from './database';
import {controller} from './controller';

const server = http.createServer((req, res) => {
  logger.debug('%s %s', req.method, req.url);
  // enable CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', '*');
  controller.route(req, res);
});
server.on('listening', () => {
  const address = server.address();
  const addressString = typeof address == 'string' ? address : `${address.address}:${address.port}`;
  logger.info('server listening on http://%s', addressString);
});
server.timeout = 10 * 60 * 1000; // defaults to 2 * 60 * 1000 = 120000 (2 minutes)

export function main() {
  const argvparser = optimist
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

  const argv = argvparser.argv;
  logger.level = argv.verbose ? Level.debug : Level.info;

  if (argv.help) {
    argvparser.showHelp();
  }
  else if (argv.version) {
    console.log(require('./package').version);
  }
  else {
    db.createDatabaseIfNotExists(error => {
      if (error) throw error;

      executePatches(db, '_migrations', path.join(__dirname, 'migrations'), patchError => {
        if (patchError) throw patchError;

        server.listen(argv.port, argv.hostname);
      });
    });
  }
}
