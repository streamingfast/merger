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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewMerger_SunnyPath(t *testing.T) {
	bundler := NewBundler(5, 5)

	merger := NewMerger(bundler, time.Second, "", nil, nil, nil, nil, "")

	merger.fetchMergedFileFunc = func(lowBlockNum uint64) ([]*OneBlockFile, error) {
		return nil, fmt.Errorf("nada")
	}

	srcOneBlockFiles := []*OneBlockFile{
		MustTestNewOneBlockFile("0000000001-20210728T105016.01-00000001a-00000000a-0"),
		MustTestNewOneBlockFile("0000000002-20210728T105016.02-00000002a-00000001a-0"),
		MustTestNewOneBlockFile("0000000003-20210728T105016.03-00000003a-00000002a-0"),
		MustTestNewOneBlockFile("0000000004-20210728T105016.06-00000004a-00000003a-2"),
		MustTestNewOneBlockFile("0000000006-20210728T105016.08-00000006a-00000004a-2"),
	}

	merger.fetchOneBlockFiles = func(ctx context.Context) (oneBlockFiles []*OneBlockFile, err error) {
		defer merger.Shutdown(nil)
		return srcOneBlockFiles, nil

	}

	var deletedFiles []*OneBlockFile
	merger.deleteFilesFunc = func(oneBlockFiles []*OneBlockFile) {
		deletedFiles = append(deletedFiles, oneBlockFiles...)
	}

	var mergedFiles []*OneBlockFile
	merger.mergeUploadFunc = func(inclusiveLowerBlock uint64, oneBlockFiles []*OneBlockFile) (err error) {
		mergedFiles = oneBlockFiles
		return nil
	}

	err := merger.launch()
	require.NoError(t, err)

	require.Len(t, deletedFiles, 1)
	require.Equal(t, deletedFiles, srcOneBlockFiles[0:1])
	require.Len(t, mergedFiles, 4)
	require.Equal(t, mergedFiles, srcOneBlockFiles[0:4])
}

func TestNewMerger_SunnyPath_With_MergeFile_Already_Exist(t *testing.T) {
	bundler := NewBundler(5, 5)

	merger := NewMerger(bundler, time.Second, "", nil, nil, nil, nil, "")

	mergeFiles := map[uint64][]*OneBlockFile{
		0: {
			MustTestNewMergedOneBlockFile("0000000001-20210728T105016.01-00000001a-00000000a-0"),
			MustTestNewMergedOneBlockFile("0000000002-20210728T105016.02-00000002a-00000001a-0"),
			MustTestNewMergedOneBlockFile("0000000003-20210728T105016.03-00000003a-00000002a-0"),
			MustTestNewMergedOneBlockFile("0000000004-20210728T105016.06-00000004a-00000003a-2"),
		},
	}

	var mergeFilesFetched []uint64
	merger.fetchMergedFileFunc = func(lowBlockNum uint64) ([]*OneBlockFile, error) {
		mergeFilesFetched = append(mergeFilesFetched, lowBlockNum)
		oneBlockFile, found := mergeFiles[lowBlockNum]
		if !found {
			return nil, fmt.Errorf("nada")
		}
		return oneBlockFile, nil
	}

	merger.fetchOneBlockFiles = func(ctx context.Context) (oneBlockFiles []*OneBlockFile, err error) {
		defer merger.Shutdown(nil)
		return nil, nil
	}

	merger.deleteFilesFunc = func(oneBlockFiles []*OneBlockFile) {
		t.Fatalf("should not have been call")
	}

	merger.mergeUploadFunc = func(inclusiveLowerBlock uint64, oneBlockFiles []*OneBlockFile) (err error) {
		t.Fatalf("should not have been call")
		return nil
	}

	err := merger.launch()
	require.NoError(t, err)

	require.Equal(t, mergeFilesFetched, []uint64{0})
}

func TestNewMerger_SunnyPath_With_Bootstrap(t *testing.T) {
	bundler := NewBundler(5, 5)

	merger := NewMerger(bundler, time.Second, "", nil, nil, nil, nil, "")

	mergeFiles := map[uint64][]*OneBlockFile{
		0: {
			MustTestNewMergedOneBlockFile("0000000001-20210728T105016.01-00000001a-00000000a-0"),
			MustTestNewMergedOneBlockFile("0000000002-20210728T105016.02-00000002a-00000001a-0"),
			MustTestNewMergedOneBlockFile("0000000003-20210728T105016.03-00000003a-00000002a-0"),
			MustTestNewMergedOneBlockFile("0000000004-20210728T105016.06-00000004a-00000003a-2"),
		},
	}

	var mergeFilesFetched []uint64
	merger.fetchMergedFileFunc = func(lowBlockNum uint64) ([]*OneBlockFile, error) {
		mergeFilesFetched = append(mergeFilesFetched, lowBlockNum)
		oneBlockFile, found := mergeFiles[lowBlockNum]
		if !found {
			return nil, fmt.Errorf("nada")
		}
		return oneBlockFile, nil
	}

	merger.fetchOneBlockFiles = func(ctx context.Context) (oneBlockFiles []*OneBlockFile, err error) {
		defer merger.Shutdown(nil)
		return nil, nil

	}

	merger.deleteFilesFunc = func(oneBlockFiles []*OneBlockFile) {
		t.Fatalf("should not have been call")
	}

	merger.mergeUploadFunc = func(inclusiveLowerBlock uint64, oneBlockFiles []*OneBlockFile) (err error) {
		t.Fatalf("should not have been call")
		return nil
	}

	err := bundler.Boostrap(merger.fetchMergedFileFunc)
	require.NoError(t, err)

	err = merger.launch()
	require.NoError(t, err)

	require.Equal(t, mergeFilesFetched, []uint64{0, 0}) //one time from the bootstrap and one time from main loop
}
