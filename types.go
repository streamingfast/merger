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
	"io/ioutil"
	"time"

	"github.com/dfuse-io/dstore"
)

type OneBlockFile struct {
	name       string
	blockTime  time.Time
	id         string
	num        uint64
	previousID string
	data       []byte
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
	out, err := s.OpenObject(ctx, f.name)
	if err != nil {
		return err
	}
	defer out.Close()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	f.data, err = ioutil.ReadAll(out)
	return err
}
