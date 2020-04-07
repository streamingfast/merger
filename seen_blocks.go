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
	M           map[string]bool
	filename    string
	keepSize    uint64
	HighestSeen uint64
}

func NewSeenBlockCache(filename string, keepSize uint64) (c *SeenBlockCache) {
	var err error
	c, err = loadSeenBlocks(filename)
	if err != nil {
		zlog.Info("cannot load seen_block_cache", zap.String("filename", filename), zap.Error(err))
		c = &SeenBlockCache{
			M: make(map[string]bool),
		}
	} else {
		zlog.Info("loaded seen_block_cache", zap.String("filename", filename), zap.Int("length", len(c.M)))
	}
	c.filename = filename
	c.keepSize = keepSize
	return
}

func (c *SeenBlockCache) Reset() {
	c.M = make(map[string]bool)
	c.HighestSeen = 0
}

func (c *SeenBlockCache) IsTooOld(num uint64) bool {
	if num < c.lowBoundary() {
		return true
	}
	return false
}
func (c *SeenBlockCache) SeenBefore(filename string) bool {
	if _, ok := c.M[filename]; ok {
		return true
	}
	return false
}

func (c *SeenBlockCache) Add(filename string) {
	num := fileToNum(filename)
	if num > c.HighestSeen {
		c.HighestSeen = num
	}
	c.M[filename] = true
}

func fileToNum(filename string) uint64 {
	blockNum, _, _, _, _ := parseFilename(filename)
	return blockNum
}

func (c *SeenBlockCache) lowBoundary() uint64 {
	if c.HighestSeen <= c.keepSize {
		return 0
	}

	return c.HighestSeen - c.keepSize

}

func (c *SeenBlockCache) Truncate() {
	for filename := range c.M {
		if fileToNum(filename) < c.lowBoundary() {
			delete(c.M, filename)
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
	f, err := os.Create(c.filename)
	if err != nil {
		return err
	}
	defer f.Close()
	dataEncoder := gob.NewEncoder(f)
	return dataEncoder.Encode(c)
}
