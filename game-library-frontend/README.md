# Game Library Frontend

Set the URLs for the GLS and UMS in `app/src/lib/setup.js` to the ones you want to use. E.g., the official public ones are:
```
const gls_url = 'https://vassalengine.org/api/gls/v1';
const ums_url = 'https://vassalengine.org/api/ums/v1';
```

Set the location of the site assets in `src/main.rs`:
```
const SITE_DIR: &str = "../../../../site";
```

Copy `config.toml.sample` to `config.toml` and adjust as desred.

To build:
```sh
$ cd app
$ npm install
$ npm run build
$ cd ..
$ cargo build

```

To test:
```sh
cd app
npm run test 
cd ..
cargo test
```

To run:
```sh
$ cargo run
```
