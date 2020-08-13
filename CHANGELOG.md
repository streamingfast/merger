# Change log

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased
### Changed
* Merger now deletes one-block-files that it has seen before exactly like the ones that are passed MaxFixableFork, based on DeleteBlocksBefore

### Added
* Config: `OneBlockDeletionThreads` to control how many one-block-files will be deleted in parallel on storage, 10 is a sane default, 1 is the minimum.
* Config: `MaxOneBlockOperationsBatchSize` to control how many files ahead to we read (also how many files we can put in deleting queue at a time.) Should be way more than the number of files that we need to merge in case of forks, 2000 is a sane default, 250 is the minimum

### Improved
* Logging of OneBlockFile deletion now only called once per delete batch

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
