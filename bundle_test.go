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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func contiguousBlocksGetter(from, to uint64) func() map[string]*OneBlockFile {
	return func() map[string]*OneBlockFile {
		f := make(map[string]*OneBlockFile)
		for i := from; i <= to; i++ {
			f[numToID(i, "a")] = testBlkFile(i, "a")
		}
		return f
	}
}

func TestIsComplete(t *testing.T) {

	tests := []struct {
		name           string
		fileListGetter func() map[string]*OneBlockFile
		bundle         *Bundle
		expectComplete bool
	}{
		{
			name:           "golden",
			fileListGetter: contiguousBlocksGetter(100, 199),
			bundle: &Bundle{
				lowerBlock:   100,
				upperBlockID: numToID(199, "a"),
			},
			expectComplete: true,
		},
		{
			name:           "beginning",
			fileListGetter: contiguousBlocksGetter(2, 99),
			bundle: &Bundle{
				lowerBlock:   0,
				upperBlockID: numToID(99, "a"),
			},
			expectComplete: true,
		},
		{
			name:           "incomplete",
			fileListGetter: contiguousBlocksGetter(100, 180),
			bundle: &Bundle{
				lowerBlock:   100,
				upperBlockID: numToID(199, "a"),
			},
			expectComplete: false,
		},
		{
			name: "hole",
			fileListGetter: func() map[string]*OneBlockFile {
				partOne := contiguousBlocksGetter(100, 180)()
				partTwo := contiguousBlocksGetter(182, 199)()

				for k, v := range partTwo {
					partOne[k] = v
				}
				return partOne
			},
			bundle: &Bundle{
				lowerBlock:   100,
				upperBlockID: numToID(199, "a"),
			},
			expectComplete: false,
		},

		{
			name:           "wrong_fork",
			fileListGetter: contiguousBlocksGetter(100, 199),
			bundle: &Bundle{
				lowerBlock:   100,
				upperBlockID: numToID(199, "b"),
			},
			expectComplete: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b := test.bundle
			b.fileList = test.fileListGetter()
			assert.Equal(t, test.expectComplete, b.isComplete())
		})
	}

}

func numToID(num uint64, fork string) string {
	return fmt.Sprintf("%08x%s", num, fork)
}

func testBlkFile(num uint64, fork string) *OneBlockFile {
	id := numToID(num, "a")
	prev := numToID(num-1, "a")
	blocktime := time.Now().Add((time.Duration(num) - 1000) * time.Second)
	return &OneBlockFile{
		name:       fmt.Sprintf("%s-%s-%s", id, prev, blocktime),
		blockTime:  blocktime,
		id:         id,
		previousID: prev,
		num:        num,
	}
}
