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

	"github.com/dfuse-io/dstore"
	"go.uber.org/zap"
)

// findNextBaseBlock will return an error if there is a gap found ...
func (m *Merger) FindNextBaseBlock() (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ListFilesTimeout)
	defer cancel()

	prefix := highestFilePrefix(ctx, m.destStore, m.minimalBlockNum, m.chunkSize)
	zlog.Debug("find_next_base looking with prefix", zap.String("prefix", prefix))
	var lastNumber uint64
	foundAny := false
	err := m.destStore.Walk(ctx, prefix, ".tmp", func(filename string) error {
		fileNumberVal, err := strconv.ParseUint(filename, 10, 32)
		if err != nil {
			zlog.Warn("findNextBaseBlock skipping unknown file", zap.String("filename", filename))
			return nil
		}
		fileNumber := fileNumberVal
		if fileNumber < m.minimalBlockNum {
			return nil
		}
		foundAny = true

		if lastNumber == 0 {
			lastNumber = fileNumber
		} else {
			if fileNumber != lastNumber+m.chunkSize {
				return fmt.Errorf("hole was found between %d and %d", lastNumber, fileNumber)
			}
			lastNumber = fileNumber
		}
		return nil
	})
	if err != nil {
		zlog.Error("find_next_base_block found hole", zap.Error(err))
	}
	if !foundAny {
		return m.minimalBlockNum, err
	}

	return lastNumber + m.chunkSize, err
}

func getLeadingZeroes(blockNum uint64) (leadingZeros int) {
	zlog.Debug("looking for filename", zap.String("filename", fileNameForBlocksBundle(int64(blockNum))))
	for i, digit := range fileNameForBlocksBundle(int64(blockNum)) {
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

func scanForHighestPrefix(ctx context.Context, store dstore.Store, chunckSize, blockNum uint64, lastPrefix string, level int) string {
	if level == -1 {
		return lastPrefix
	}

	inc := chunckSize * uint64(math.Pow10(level))
	for {
		b := blockNum + inc
		leadingZeroes := strings.Repeat("0", getLeadingZeroes(b))
		prefix := leadingZeroes + strconv.Itoa(int(b))
		exist := fileExistWithPrefix(ctx, prefix, store)
		zlog.Debug("file with prefix", zap.String("prefix", prefix), zap.Bool("exist", exist))
		if !exist {
			break
		}
		blockNum = b
		lastPrefix = prefix
	}
	return scanForHighestPrefix(ctx, store, chunckSize, blockNum, lastPrefix, level-1)
}

func highestFilePrefix(ctx context.Context, store dstore.Store, minimalBlockNum uint64, chuckSize uint64) (filePrefix string) {
	leadingZeroes := strings.Repeat("0", getLeadingZeroes(minimalBlockNum))
	blockNumStr := strconv.Itoa(int(minimalBlockNum))
	filePrefix = leadingZeroes + blockNumStr

	if !fileExistWithPrefix(ctx, filePrefix, store) {
		// prefix of minimalBlockNum not found.
		// we consider it has the highest file prefix
		zlog.Info("prefix of minimalBlockNum not found. we consider it has the highest file prefix")
		return
	}

	filePrefix = scanForHighestPrefix(ctx, store, chuckSize, minimalBlockNum, filePrefix, 4)
	return
}

func fileExistWithPrefix(ctx context.Context, filePrefix string, s dstore.Store) bool {
	needZeros := 10 - len(filePrefix)
	resultFileName := filePrefix + strings.Repeat("0", needZeros)
	exists, err := s.FileExists(ctx, resultFileName)
	if err != nil {
		zlog.Error("looking for file existence on archive store", zap.Error(err))
		return false
	}
	if exists {
		return true
	}
	return false
}

func fileNameForBlocksBundle(blockNum int64) string {
	return fmt.Sprintf("%010d", blockNum)
}
