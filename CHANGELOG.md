# Change log

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [v0.0.1]
### Changed
* `--listen-grpc-addr` now is `--grpc-listen-addr`

### Removed
* Removed the `protocol`, merger is not `protocol` agnostic 
* Removed EnableReadinessProbe option in config, it is now the only behavior

### Improved
* Logging was adjust at many places
* context now used in every dstore call for better timeout handling

## 2020-03-21

### Changed

* License changed to Apache 2.0
