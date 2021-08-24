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
	"io"
	"io/ioutil"
	"time"

	"github.com/streamingfast/dstore"
)

type OneBlockFile struct {
	canonicalName string
	filenames     map[string]struct{}
	blockTime     time.Time
	id            string
	num           uint64
	libNum        uint64
	previousID    string
	data          []byte
	merged        bool
}

func MustNewOneBlockFile(fileName string) *OneBlockFile {
	blockNum, blockTime, blockID, previousBlockID, libNum, canonicalName, err := parseFilename(fileName)
	if err != nil {
		panic(err)
	}
	return &OneBlockFile{
		canonicalName: canonicalName,
		filenames: map[string]struct{}{
			fileName: Empty,
		},
		blockTime:  blockTime,
		id:         blockID,
		num:        blockNum,
		previousID: previousBlockID,
		libNum:     libNum,
	}
}

func (f *OneBlockFile) Data(ctx context.Context, s dstore.Store) ([]byte, error) {
	if len(f.data) == 0 {
		err := f.downloadFile(ctx, s)
		if err != nil {
			return nil, err
		}
	}
	return f.data, nil
}

func (f *OneBlockFile) downloadFile(ctx context.Context, s dstore.Store) error {
	var err error
	for filename := range f.filenames { // will try to get data from any of those files
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

		f.data, err = ioutil.ReadAll(out)
		if err == nil {
			return nil
		}
	}
	return err // last error seen during attempts (OpenObject or ReadAll)
}
