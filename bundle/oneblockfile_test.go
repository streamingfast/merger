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

package bundle

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseFilenames(t *testing.T) {
	lib := func(num uint64) *uint64 { lib := num; return &lib }
	tests := []struct {
		name                        string
		filename                    string
		expectBlockNum              uint64
		expectBlockTime             time.Time
		expectBlockIDSuffix         string
		expectPreviousBlockIDSuffix string
		expectCanonicalName         string
		expectError                 error
		expectLibNum                *uint64
	}{
		{
			name:        "invalid",
			filename:    "invalid-filename",
			expectError: fmt.Errorf("wrong filename format: \"invalid-filename\""),
		},
		{
			name:                        "without lib",
			filename:                    "0000000100-20170701T122141.0-24a07267-e5914b39",
			expectBlockNum:              100,
			expectLibNum:                nil,
			expectBlockTime:             mustParseTime("20170701T122141.0"),
			expectBlockIDSuffix:         "24a07267",
			expectPreviousBlockIDSuffix: "e5914b39",
			expectCanonicalName:         "0000000100-20170701T122141.0-24a07267-e5914b39",
		},
		{
			name:                        "with suffix",
			filename:                    "0000000100-20170701T122141.0-24a07267-e5914b39-90-mind1",
			expectBlockNum:              100,
			expectLibNum:                lib(90),
			expectBlockTime:             mustParseTime("20170701T122141.0"),
			expectBlockIDSuffix:         "24a07267",
			expectPreviousBlockIDSuffix: "e5914b39",
			expectCanonicalName:         "0000000100-20170701T122141.0-24a07267-e5914b39-90",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			blkNum, blkTime, blkID, prevBlkID, libNum, name, err := parseFilename(test.filename)
			if test.expectError != nil {
				require.Equal(t, err, test.expectError)
				return
			}
			require.Nil(t, err)
			assert.Equal(t, test.expectBlockNum, blkNum)
			assert.Equal(t, test.expectLibNum, libNum)
			assert.Equal(t, test.expectBlockTime, blkTime)
			assert.Equal(t, test.expectBlockIDSuffix, blkID)
			assert.Equal(t, test.expectPreviousBlockIDSuffix, prevBlkID)
			assert.Equal(t, test.expectCanonicalName, name)

		})
	}
}

func mustParseTime(in string) time.Time {
	t, err := time.Parse("20060102T150405.999999", in)
	if err != nil {
		panic("invalid parsetime")
	}
	return t
}
