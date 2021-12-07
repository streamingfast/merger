// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merger

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

// FindNextBaseMergedBlock will return an error if there is a gap found ...
func FindNextBaseMergedBlock(mergedBlocksStore dstore.Store, chunkSize uint64) (uint64, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ListFilesTimeout)
	defer cancel()
	foundAny := false
	prefix, err := highestFilePrefix(ctx, mergedBlocksStore, uint64(0), chunkSize)
	if err != nil {
		return 0, foundAny, err
	}
	zlog.Debug("find next base merged block, looking with prefix", zap.String("prefix", prefix))
	var lastNumber uint64
	err = mergedBlocksStore.Walk(ctx, prefix, ".tmp", func(filename string) error {
		fileNumberVal, err := strconv.ParseUint(filename, 10, 32)
		if err != nil {
			zlog.Warn("find next base merged block, skipping unknown file", zap.String("filename", filename))
			return nil
		}
		fileNumber := fileNumberVal
		foundAny = true

		if fileNumber != lastNumber+chunkSize && lastNumber != 0 {
			zlog.Warn("hole was found while walking", zap.Uint64("last_number", lastNumber), zap.Uint64("file_number", fileNumber))
		}
		lastNumber = fileNumber
		return nil
	})
	if err == context.DeadlineExceeded {
		err = nil
	}
	if !foundAny {
		return 0, foundAny, err
	}

	return lastNumber + chunkSize, foundAny, err
}

func getLeadingZeroes(blockNum uint64) (leadingZeros int) {
	filename := fileNameForBlocksBundle(blockNum)
	for i, digit := range filename {
		if digit == '0' && leadingZeros == 0 {
			continue
		}
		if leadingZeros == 0 {
			leadingZeros = i
			return
		}
	}
	return
}

func scanForHighestPrefix(ctx context.Context, store dstore.Store, chunckSize, blockNum uint64, lastPrefix string, level int) (string, error) {
	if level == -1 {
		return lastPrefix, nil
	}

	inc := chunckSize * uint64(math.Pow10(level))
	for {
		b := blockNum + inc
		leadingZeroes := strings.Repeat("0", getLeadingZeroes(b))
		prefix := leadingZeroes + strconv.Itoa(int(b))
		exist, err := fileExistWithPrefix(ctx, prefix, store)
		if err != nil {
			return "", err
		}
		zlog.Debug("file with prefix", zap.String("prefix", prefix), zap.Bool("exist", exist))
		if !exist {
			break
		}
		blockNum = b
		lastPrefix = prefix
	}
	return scanForHighestPrefix(ctx, store, chunckSize, blockNum, lastPrefix, level-1)
}

func highestFilePrefix(ctx context.Context, mergedBlocksStore dstore.Store, minimalBlockNum uint64, chuckSize uint64) (filePrefix string, err error) {
	leadingZeroes := strings.Repeat("0", getLeadingZeroes(minimalBlockNum))
	blockNumStr := strconv.Itoa(int(minimalBlockNum))
	filePrefix = leadingZeroes + blockNumStr

	//var exists bool
	//exists, err = fileExistWithPrefix(ctx, filePrefix, mergedBlocksStore)
	//if err != nil {
	//	return
	//}
	//if !exists {
	//	zlog.Info("prefix of minimal block num not found in merged blocks, we consider it has the highest file prefix", zap.String("file_prefix", filePrefix))
	//	return
	//}

	filePrefix, err = scanForHighestPrefix(ctx, mergedBlocksStore, chuckSize, minimalBlockNum, filePrefix, 4)
	return
}

func fileExistWithPrefix(ctx context.Context, filePrefix string, mergedBlocksStore dstore.Store) (bool, error) {
	needZeros := 10 - len(filePrefix)
	resultFileName := filePrefix + strings.Repeat("0", needZeros)
	return mergedBlocksStore.FileExists(ctx, resultFileName)
}

func fileNameForBlocksBundle(blockNum uint64) string {
	return fmt.Sprintf("%010d", blockNum)
}
