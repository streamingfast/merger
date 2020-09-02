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

func TestAdd(t *testing.T) {

	blockFile100 := &OneBlockFile{
		canonicalName: "0000000100-20170701T122141.0-24a07267-e5914b39",
		filenames:     []string{"0000000100-20170701T122141.0-24a07267-e5914b39"},
		blockTime:     mustParseTime("20170701T122141.0"),
		id:            "24a07267",
		num:           100,
		previousID:    "e5914b39",
	}
	blockFile101Mindread1 := &OneBlockFile{
		canonicalName: "0000000101-20170701T122141.5-dbda3f44-24a07267",
		filenames:     []string{"0000000101-20170701T122141.5-dbda3f44-24a07267-mindread1"},
		blockTime:     mustParseTime("20170701T122141.5"),
		id:            "dbda3f44",
		num:           101,
		previousID:    "24a07267",
	}
	blockFile101Mindread2 := &OneBlockFile{
		canonicalName: "0000000101-20170701T122141.5-dbda3f44-24a07267",
		filenames:     []string{"0000000101-20170701T122141.5-dbda3f44-24a07267-mindread2"},
		blockTime:     mustParseTime("20170701T122141.5"),
		id:            "dbda3f44",
		num:           101,
		previousID:    "24a07267",
	}

	tests := []struct {
		name           string
		addFiles       []*OneBlockFile
		expectFileList map[string]*OneBlockFile
	}{
		{
			name: "two different files",
			addFiles: []*OneBlockFile{
				blockFile100,
				blockFile101Mindread1,
			},
			expectFileList: map[string]*OneBlockFile{
				"0000000100-20170701T122141.0-24a07267-e5914b39": blockFile100,
				"0000000101-20170701T122141.5-dbda3f44-24a07267": blockFile101Mindread1,
			},
		},
		{
			name: "two equivalent files",
			addFiles: []*OneBlockFile{
				blockFile101Mindread1,
				blockFile101Mindread2,
			},
			expectFileList: map[string]*OneBlockFile{
				"0000000101-20170701T122141.5-dbda3f44-24a07267": &OneBlockFile{
					canonicalName: blockFile101Mindread1.canonicalName,
					filenames:     append(blockFile101Mindread1.filenames, blockFile101Mindread2.filenames[0]),
					blockTime:     blockFile101Mindread1.blockTime,
					id:            blockFile101Mindread1.id,
					num:           blockFile101Mindread1.num,
					previousID:    blockFile101Mindread1.previousID,
				},
			},
		},
		{
			name: "a single file, two equivalent files, then repeat",
			addFiles: []*OneBlockFile{
				blockFile100,
				blockFile101Mindread1,
				blockFile101Mindread2,
				blockFile100,
				blockFile101Mindread1,
				blockFile101Mindread2,
			},
			expectFileList: map[string]*OneBlockFile{
				"0000000100-20170701T122141.0-24a07267-e5914b39": blockFile100,
				"0000000101-20170701T122141.5-dbda3f44-24a07267": &OneBlockFile{
					canonicalName: blockFile101Mindread1.canonicalName,
					filenames:     append(blockFile101Mindread1.filenames, blockFile101Mindread2.filenames[0]),
					blockTime:     blockFile101Mindread1.blockTime,
					id:            blockFile101Mindread1.id,
					num:           blockFile101Mindread1.num,
					previousID:    blockFile101Mindread1.previousID,
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b := &Bundle{
				fileList: make(map[string]*OneBlockFile),
			}
			for _, f := range test.addFiles {
				b.add(f.filenames[0],
					f.num,
					f.blockTime,
					f.id,
					f.previousID,
					f.canonicalName,
				)
			}
			assert.Equal(t, test.expectFileList, b.fileList)
		})
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
		canonicalName: fmt.Sprintf("%s-%s-%s", id, prev, blocktime),
		filenames:     []string{fmt.Sprintf("%s-%s-%s", id, prev, blocktime)},
		blockTime:     blocktime,
		id:            id,
		previousID:    prev,
		num:           num,
	}
}
