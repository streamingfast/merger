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
	"encoding/gob"
	"os"

	"go.uber.org/zap"
)

type SeenBlockCache struct {
	OneBlockFiles map[string]bool
	filename      string // optional, in-memory mode if not set
	maxSize       uint64

	HighestBlockNum uint64
	LowestBlockNum  uint64
}

func NewSeenBlockCacheInMemory(startBlockNum, maxSize uint64) (c *SeenBlockCache) {
	var highestSeen uint64
	if startBlockNum > 0 {
		highestSeen = startBlockNum - 1
	}
	return &SeenBlockCache{
		OneBlockFiles:   make(map[string]bool),
		maxSize:         maxSize,
		HighestBlockNum: highestSeen,
	}
}

func NewSeenBlockCacheFromFile(filename string, maxSize uint64) (c *SeenBlockCache) {
	var err error
	c, err = loadSeenBlocks(filename)
	if err != nil {
		zlog.Info("cannot load seen_block_cache", zap.String("filename", filename), zap.Error(err))
		c = &SeenBlockCache{
			OneBlockFiles: make(map[string]bool),
		}
	} else {
		zlog.Info("loaded seen_block_cache", zap.String("filename", filename), zap.Int("length", len(c.OneBlockFiles)))
	}
	c.filename = filename
	c.maxSize = maxSize
	c.adjustLowestBlockNum()
	return
}

func NewSeenBlockCacheFromNewFile(filename string, maxSize uint64) (c *SeenBlockCache) {

	zlog.Info("creating new seen_block_cache", zap.String("filename", filename), zap.Uint64("max_size", maxSize))
	c = &SeenBlockCache{
		filename:      filename,
		OneBlockFiles: make(map[string]bool),
		maxSize:       maxSize,
	}
	c.adjustLowestBlockNum()
	return c
}

// adjustLowestBlockNum will never lower the lowestSeen value, it can only go up
func (c *SeenBlockCache) adjustLowestBlockNum() {
	if c.HighestBlockNum < c.maxSize {
		return
	}

	newLowestBlockNum := c.HighestBlockNum - c.maxSize
	if newLowestBlockNum > c.LowestBlockNum {
		c.LowestBlockNum = newLowestBlockNum
	}

}

func (c *SeenBlockCache) IsTooOld(num uint64) bool {
	if num < c.LowestBlockNum {
		return true
	}
	return false
}

func (c *SeenBlockCache) SeenBefore(filename string) bool {
	if _, ok := c.OneBlockFiles[filename]; ok {
		return true
	}
	return false
}

func (c *SeenBlockCache) Add(filename string) {
	num := fileToNum(filename)
	if num > c.HighestBlockNum {
		c.HighestBlockNum = num
	}
	c.OneBlockFiles[filename] = true
}

func fileToNum(filename string) uint64 {
	blockNum, _, _, _, _, _ := parseFilename(filename)
	return blockNum
}

func (c *SeenBlockCache) Truncate() {
	c.adjustLowestBlockNum()
	for filename := range c.OneBlockFiles {
		if fileToNum(filename) < c.LowestBlockNum {
			delete(c.OneBlockFiles, filename)
		}
	}
}

func loadSeenBlocks(filename string) (decoded *SeenBlockCache, err error) {
	f, err := os.Open(filename)
	if err != nil {
		return
	}
	defer f.Close()

	dataDecoder := gob.NewDecoder(f)
	err = dataDecoder.Decode(&decoded)
	return
}

func (c *SeenBlockCache) Save() error {
	if c.filename == "" { // in memory mode
		return nil
	}
	f, err := os.Create(c.filename)
	if err != nil {
		return err
	}
	defer f.Close()
	dataEncoder := gob.NewEncoder(f)
	return dataEncoder.Encode(c)
}
