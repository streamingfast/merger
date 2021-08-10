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
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWalkGS(t *testing.T) {
	t.Skip("does not work on cloudbuild for some reason... probably permissions !")

	storePath := fmt.Sprintf("gs://eoscanada-public-nodeos-archive/dev/%d", time.Now().UnixNano())

	writtenFiles := []string{"0000000000", "0000000100", "0000000200"} // archivestore doesn't require file suffix
	expectedFiles := []string{"0000000000", "0000000100", "0000000200"}

	s, err := dstore.NewDBinStore(storePath)
	require.NoError(t, err)

	for _, filename := range writtenFiles {
		err := s.WriteObject(context.Background(), filename, strings.NewReader(""))
		require.NoError(t, err)
	}

	files := []string{}
	s.Walk(context.Background(), "", ".tmp", func(filename string) error {
		files = append(files, filename)
		return nil
	})
	assert.EqualValues(t, expectedFiles, files)
}

func TestWalkFS(t *testing.T) {
	t.Skip("hmmm.. just testing the obvious.. testing dstore really?")

	tmpdir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	writtenFiles := []string{"0000000000.jsonl.gz", "0000000100.jsonl.gz", "0000000200.jsonl.gz"} // full filename for ioutil.WriteFile()
	expectedFiles := []string{"0000000000.jsonl.gz", "0000000100.jsonl.gz", "0000000200.jsonl.gz"}
	for _, filename := range writtenFiles {
		ioutil.WriteFile(path.Join(tmpdir, filename), []byte{}, 0644)
	}
	fmt.Println(tmpdir)
	s, err := dstore.NewDBinStore(tmpdir)
	require.NoError(t, err)
	files := []string{}
	s.Walk(context.Background(), "", ".tmp", func(filename string) error {
		files = append(files, filename)
		return nil
	})
	assert.EqualValues(t, expectedFiles, files)
}

func TestFindNextBaseBlock(t *testing.T) {

	tests := []struct {
		name              string
		writtenFiles      []string
		minimalBlockNum   uint64
		chunkSize         uint64
		expectedBaseBlock uint64
	}{
		{
			name:              "zero",
			writtenFiles:      []string{},
			chunkSize:         100,
			minimalBlockNum:   0,
			expectedBaseBlock: 0,
		},
		{
			name:              "simple",
			writtenFiles:      []string{"0000000000", "0000000100", "0000000200"},
			chunkSize:         100,
			minimalBlockNum:   0,
			expectedBaseBlock: 300,
		},
		{
			name:              "round_minimal_num",
			writtenFiles:      []string{"0000000100", "0000003400", "0000010000", "0000010100", "0000010200"},
			chunkSize:         100,
			minimalBlockNum:   10000,
			expectedBaseBlock: 10300,
		},
		{
			name:              "specific_minimal_num",
			writtenFiles:      []string{"0000000100", "0000003400", "0000010000", "0000010200", "0000010300"},
			chunkSize:         100,
			minimalBlockNum:   10200,
			expectedBaseBlock: 10400,
		},
		{
			name:              "complex_minimal_num",
			writtenFiles:      []string{"0000000100", "0000003400", "0000010000", "0008976500", "0008976600"},
			chunkSize:         100,
			minimalBlockNum:   8976500,
			expectedBaseBlock: 8976700,
		},
		{
			name:              "complex_minimal_num with chunck size 50",
			writtenFiles:      []string{"0000000100", "0000000150", "0000015000", "0008976500", "0008976550", "0008976600", "0008976650"},
			chunkSize:         50,
			minimalBlockNum:   8976500,
			expectedBaseBlock: 8976700,
		},
		{
			name:              "complex_minimal_num with chunck size 3",
			writtenFiles:      []string{"0000000100", "0000000103", "0000000106", "0000000507", "0000000510", "0000000513"},
			chunkSize:         3,
			minimalBlockNum:   507,
			expectedBaseBlock: 516,
		},
		{
			name:              "absent_minimal_num",
			writtenFiles:      []string{"0000000100", "0000003400", "0000010000"},
			chunkSize:         100,
			minimalBlockNum:   8976500,
			expectedBaseBlock: 8976500,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tmpdir, err := ioutil.TempDir("", "")
			defer os.RemoveAll(tmpdir)
			require.NoError(t, err)

			s, err := dstore.NewDBinStore(tmpdir)
			require.NoError(t, err)

			for _, filename := range test.writtenFiles {
				err := s.WriteObject(context.Background(), filename, strings.NewReader(""))
				require.NoError(t, err)
			}

			i, err := FindNextBaseMergedBlock(s, test.minimalBlockNum, test.chunkSize)
			require.NoError(t, err)

			assert.Equal(t, int(test.expectedBaseBlock), int(i))

		})
	}
}
