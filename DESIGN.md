# DESIGN

## OneBlock files naming

{TIMESTAMP}-{BLOCKNUM}-{BLOCKIDSUFFIX}-{PREVIOUSIDSUFFIX}.json.gz

* TIMESTAMP: YYYYMMDDThhmmss.{0|5} where 0 and 5 are the possible values for 500-millisecond increments..
* BLOCKNUM: 0-padded block number
* BLOCKIDSUFFIX: [:8] from block ID
* PREVIOUSIDSUFFIX: [:8] previous_id for block

Example:
* 20170701T122141.0-0000000100-24a07267-e5914b39.json.gz
* 20170701T122141.5-0000000101-dbda3f44-09f6d693.json.gz

 fmt.Sprintf("%s.%01d", t.Format("20060102T150405"), t.Nanosecond()/100000000)

## Merger structure

* chunkSize == 100
* currentBaseBlockNum uint32
* currentBaseBlockTime time.Time
* fileList map[string]OneBlockFile
* wg //parallel wg for downloading...
* nextBaseBlockIDSuffix [8]string
* nextBaseBlockTime time.Time
* nextBaseBlockFoundAt time.Time // we set this to Now() when we find the nextBaseBlock and its nextBaseBlockTime

OneBlockFile struct{
  name string
  blk hlog.*Block
}

## REAL-TIME

### init
1) list the 100blocks files, keeping map
2) go through every 100blocks segment from 0 to figure out if 100blocks segment is missing
3) determine the base block number where to start

### processing loop

#### 1. polling
A) Do we have currentBaseBlockTime known ?
* poll storage (every ... 10 seconds ?) for all one-block-file, send each filename in Triage() func

B) else (merger was just started...):
* poll storage for a one-block-file called: `blockNum-*.json.gz`
  Set the currentBaseBlockTime to the earliest timestamp for that blocknum and continue to next loop

#### 2. Triage each file from GS
Triage checks:
  if (
  * filename.blockNum < currentBaseBlockNum + chunkSize
  * filename.blockTime >= currentBaseBlockTime
  ) --> start downloading that file (go func) and put it in map for inclusion

  if (
  * filename.blockNum == currentBaseBlockNum
  ) -> set the nextBaseBlockTime to the lowest value you can find here
    -> set nextBaseBlockFoundAt to Now() when you replace this with a lower value.

#### 3. check if we must proceed with merging
* if nextBaseBlockTime != 0 && Now() && we can crawl a whole sequence from the last block up to the first blocknum > nextBaseBlockFoundAt + 25 seconds, do the merging. If not, back in the loop.

#### 4. Do the merge
* wg.Wait() to finish the downloads,
* remove the file sin the map which are => nextBaseBlockTime (there may be some files if that value changed...)
* order all the files in the map by blocktime and write their content in a jsonl 100blocks file on disk (blocks at equal blocktime can be written in whatever order...)
* upload that 100blocks file
* delete the files in the map from Google Storage
* reset your Merger's values and make currentBaseBlockNum=currentBaseBlockNum+chunkSize, currentBaseBlockTime=nextBaseBlockTime


## REPLAY

note: mindreader writes 1block files to a local folder on the same pod (shared by 2 containers...)

same thing as real-time, except:
1) "init" will simply take the startBlockNum instead of finding it from the list of 100blocks file on destination
2) "process.4.merge" will download a 100blocks file from destination and add all those blocks in the filelist map
