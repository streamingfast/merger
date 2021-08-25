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
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"time"

	"github.com/streamingfast/dstore"
)

var Empty struct{}

type OneBlockFile struct {
	CanonicalName string
	Filenames     map[string]struct{}
	BlockTime     time.Time
	ID            string
	Num           uint64
	libNum        uint64
	PreviousID    string
	MemoizeData   []byte
	Merged        bool
}

func MustNewOneBlockFile(fileName string) *OneBlockFile {
	blockNum, blockTime, blockID, previousBlockID, libNum, canonicalName, err := parseFilename(fileName)
	if err != nil {
		panic(err)
	}
	return &OneBlockFile{
		CanonicalName: canonicalName,
		Filenames: map[string]struct{}{
			fileName: Empty,
		},
		BlockTime:  blockTime,
		ID:         blockID,
		Num:        blockNum,
		PreviousID: previousBlockID,
		libNum:     libNum,
	}
}

func MustNewMergedOneBlockFile(fileName string) *OneBlockFile {
	oneBlockFile := MustNewOneBlockFile(fileName)
	oneBlockFile.Merged = true
	return oneBlockFile
}

func (f *OneBlockFile) Data(ctx context.Context, s dstore.Store) ([]byte, error) {
	if len(f.MemoizeData) == 0 {
		err := f.downloadFile(ctx, s)
		if err != nil {
			return nil, err
		}
	}
	return f.MemoizeData, nil
}

func (f *OneBlockFile) downloadFile(ctx context.Context, s dstore.Store) error {
	var err error
	for filename := range f.Filenames { // will try to get MemoizeData from any of those files
		var out io.ReadCloser
		out, err = s.OpenObject(ctx, filename)
		if err != nil {
			continue
		}
		defer out.Close()

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		f.MemoizeData, err = ioutil.ReadAll(out)
		if err == nil {
			return nil
		}
	}
	return err // last error seen during attempts (OpenObject or ReadAll)
}

// parseFilename parses file names formatted like:
// * 0000000100-20170701T122141.0-24a07267-e5914b39-99
// * 0000000101-20170701T122141.5-dbda3f44-24a07267-100-mindread1
// * 0000000101-20170701T122141.5-dbda3f44-24a07267-101-mindread2
// * 0000000102-20170701T122142.0-948232ea-dbda3f44-102
func parseFilename(filename string) (blockNum uint64, blockTime time.Time, blockIDSuffix string, previousBlockIDSuffix string, libNum uint64, canonicalName string, err error) {
	parts := strings.Split(filename, "-")
	if len(parts) < 5 || len(parts) > 6 {
		err = fmt.Errorf("wrong filename format: %q", filename)
		return
	}

	blockNumVal, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		err = fmt.Errorf("failed parsing %q: %s", parts[0], err)
		return
	}
	blockNum = blockNumVal

	blockTime, err = time.Parse("20060102T150405.999999", parts[1])
	if err != nil {
		err = fmt.Errorf("failed parsing %q: %s", parts[1], err)
		return
	}

	blockIDSuffix = parts[2]
	previousBlockIDSuffix = parts[3]

	libNumVal, err := strconv.ParseUint(parts[4], 10, 32)
	if err != nil {
		err = fmt.Errorf("failed parsing lib num %q: %s", parts[4], err)
		return
	}
	libNum = libNumVal

	canonicalName = filename
	if len(parts) == 6 {
		canonicalName = strings.Join(parts[0:5], "-")
	}
	return
}
