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
	"errors"
	"time"

	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Merger struct {
	*shutter.Shutter
	grpcListenAddr string

	io                   IOInterface
	firstStreamableBlock uint64
	logger               *zap.Logger

	timeBetweenPolling time.Duration

	timeBetweenPruning       time.Duration
	maxBlockAgeBeforePruning time.Duration

	bundler *Bundler
}

func NewMerger(
	logger *zap.Logger,
	grpcListenAddr string,
	io IOInterface,

	firstStreamableBlock uint64,
	bundleSize uint64,
	maxForkedBlockAgeBeforePruning time.Duration,
	timeBetweenPruning time.Duration,
	timeBetweenPolling time.Duration,
) *Merger {
	return &Merger{
		Shutter:                  shutter.New(),
		bundler:                  NewBundler(firstStreamableBlock, bundleSize, io),
		grpcListenAddr:           grpcListenAddr,
		io:                       io,
		maxBlockAgeBeforePruning: maxForkedBlockAgeBeforePruning,
		timeBetweenPolling:       timeBetweenPolling,
		timeBetweenPruning:       timeBetweenPruning,
		logger:                   logger,
	}
}

func (m *Merger) Run() {
	m.logger.Info("starting merger")

	if err := m.startGRPCServer(); err != nil {
		m.logger.Error("cannot start GRPC server", zap.Error(err))
		m.Shutdown(err)
		return
	}

	m.startOldFilesPruner()

	if err := m.run(); err != nil {
		m.logger.Error("merger returned error", zap.Error(err))
		m.Shutdown(err)
		return
	}
}

func (m *Merger) startOldFilesPruner() {
	m.logger.Info("starting pruning of old files (delayed by time_between_pruning)",
		zap.Duration("max_block_age_before_pruning", m.maxBlockAgeBeforePruning),
		zap.Duration("time_between_pruning", m.timeBetweenPruning),
	)

	go func() {
		time.Sleep(m.timeBetweenPruning) // do not start pruning immediately
		for {
			now := time.Now()
			ctx := context.Background()

			var toDelete []*OneBlockFile

			lowestBlockUsedByBundler := m.bundler.BaseBlockNum()
			m.io.WalkOneBlockFiles(ctx, m.firstStreamableBlock, func(obf *OneBlockFile) error {
				if obf.Num < lowestBlockUsedByBundler && time.Since(obf.BlockTime) > m.maxBlockAgeBeforePruning {
					toDelete = append(toDelete, obf)
				}
				return nil
			})
			m.io.DeleteAsync(toDelete)

			if spentTime := time.Since(now); spentTime < m.timeBetweenPruning {
				time.Sleep(m.timeBetweenPruning - spentTime)
			}
		}
	}()
}

func (m *Merger) run() error {

	ctx := context.Background()

	for {
		now := time.Now()
		if m.IsTerminating() {
			return nil
		}

		base, lib, err := m.io.NextBundle(ctx, m.bundler.baseBlockNum)
		if err != nil {
			if errors.Is(err, ErrHoleFound) {
				m.logger.Warn("found hole in merged files", zap.Error(err))
			} else {
				return err
			}
		}

		if base > m.bundler.baseBlockNum {
			logFields := []zapcore.Field{
				zap.Uint64("previous_base_block_num", m.bundler.baseBlockNum),
				zap.Uint64("new_base_block_num", base),
			}
			if lib != nil {
				logFields = append(logFields, zap.Stringer("lib", lib))
			}
			m.logger.Info("resetting bundler base block num", logFields...)
			m.bundler.Reset(base, lib)
		}

		m.io.WalkOneBlockFiles(ctx, m.bundler.baseBlockNum, func(obf *OneBlockFile) error {
			m.logger.Debug("processing block", zap.Stringer("obf", obf))
			return m.bundler.HandleBlockFile(obf)
		})

		if spentTime := time.Since(now); spentTime < m.timeBetweenPolling {
			time.Sleep(m.timeBetweenPolling - spentTime)
		}
	}
}
