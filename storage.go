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
	"fmt"
	"strconv"
	"strings"

	pbdeos "github.com/dfuse-io/pbgo/dfuse/codecs/deos"
	"github.com/dfuse-io/dstore"
	"go.uber.org/zap"
)

// findNextBaseBlock will return an error if there is a gap found ...
func (m *Merger) FindNextBaseBlock() (uint64, error) {
	prefix, err := findMinimalLastBaseBlocksBundlePrefix(m.destStore, m.minimalBlockNum)
	if err != nil {
		zlog.Info("finding minimal lastBaseBlockBundlePrefix", zap.Error(err))
		prefix = ""
	}
	zlog.Debug("find_next_base looking with prefix", zap.String("prefix", prefix))
	var lastNumber uint64
	foundAny := false
	err = m.destStore.Walk(prefix, ".tmp", func(filename string) error {
		fileNumberVal, err := strconv.ParseUint(filename, 10, 32)
		if err != nil {
			zlog.Warn("findNextBaseBlock skipping unknown file", zap.String("filename", filename))
			return nil
		}
		fileNumber := uint64(fileNumberVal)
		if fileNumber < m.minimalBlockNum {
			return nil
		}
		foundAny = true

		if lastNumber == 0 {
			lastNumber = uint64(fileNumber)
		} else {
			if fileNumber != lastNumber+m.chunkSize {
				return fmt.Errorf("hole was found between %d and %d", lastNumber, fileNumber)
			}
			lastNumber = uint64(fileNumber)
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

func getLeadingZeroesAndNextDigit(blockNum uint64) (int, int) {
	zlog.Debug("looking for filename", zap.String("filename", fileNameForBlocksBundle(int64(blockNum))))
	for i, digit := range fileNameForBlocksBundle(int64(blockNum)) {
		if digit != '0' {
			return i, int(digit - '0')
		}
	}
	return 10, 0
}

// findMinimalLastBaseBlocksBundle tries to minimize the number of network calls
// to storage, by trying incremental first digits, one at a time..
func findMinimalLastBaseBlocksBundlePrefix(s dstore.Store, minimalBlockNum uint64) (filePrefix string, err error) {
	leadingZeroes := 0
	nextDigit := 0
	if minimalBlockNum != 0 {
		leadingZeroes, nextDigit = getLeadingZeroesAndNextDigit(minimalBlockNum)
	}

	for {
		if leadingZeroes >= 8 {
			return "", fmt.Errorf("couldn't find anything...")
		}
		filePrefix = strings.Repeat("0", leadingZeroes)
		if tryPrefix(filePrefix+"1", s) {
			break
		}
		leadingZeroes++
	}

	zlog.Debug("prefixed with zeroes", zap.Int("leadingZeroes", leadingZeroes))
	for {
		for {
			attemptedDigit := nextDigit + 1
			if attemptedDigit > 9 {
				break
			}
			if !tryPrefix(fmt.Sprintf("%s%d", filePrefix, attemptedDigit), s) {
				break
			}
			nextDigit++
		}
		filePrefix = fmt.Sprintf("%s%d", filePrefix, nextDigit)
		nextDigit = 0
		if len(filePrefix) >= 6 {
			break
		}
	}
	return
}

func tryPrefix(filePrefix string, s dstore.Store) bool {
	resultFileName := filePrefix + strings.Repeat("0", 10-len(filePrefix))
	exists, err := s.FileExists(resultFileName)
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

func fileNameForBlock(block *pbdeos.Block) string {
	return fmt.Sprintf("%d-%s", block.Num(), block.ID())
}
