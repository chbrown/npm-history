# npm-history

`npm-history` is responsible for requesting and storing NPM registry download counts in a local PostgreSQL repository.
It is controlled by a minimal `HTTP` API.


## API

* `GET /packages/:name/downloads`

  Retrieve another period's (180 days) worth of downloads for the package named ':name', and return all saved downloads as JSON.
  Response format:

  ```javascript
  [
    {
      day: string; // formatted as 'YYYY-MM-DD'),
      downloads: number;
    },
    ...
  ]
  ```
* `GET /packages/averages?start=<date>&end=<date>`
  - `end` defaults to the current date.
  - `start` defaults to 60 days before the end date.

  Average the daily downloads for all packages with downloads during the given period, returning a single JSON object mapping package names to download counts.
  Response format:

  ```javascript
  {[index: string]: number}
  ```


## License

Copyright 2015 Christopher Brown. [MIT Licensed](http://chbrown.github.io/licenses/MIT/#2015).
