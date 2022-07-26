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

	"github.com/streamingfast/bstream"
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

	timeBetweenPruning   time.Duration
	pruningDistanceToLIB uint64

	bundler *Bundler
}

func NewMerger(
	logger *zap.Logger,
	grpcListenAddr string,
	io IOInterface,

	firstStreamableBlock uint64,
	bundleSize uint64,
	pruningDistanceToLIB uint64,
	timeBetweenPruning time.Duration,
	timeBetweenPolling time.Duration,
	stopBlock uint64,
) *Merger {
	m := &Merger{
		Shutter:              shutter.New(),
		bundler:              NewBundler(firstStreamableBlock, stopBlock, bundleSize, io),
		grpcListenAddr:       grpcListenAddr,
		io:                   io,
		pruningDistanceToLIB: pruningDistanceToLIB,
		timeBetweenPolling:   timeBetweenPolling,
		timeBetweenPruning:   timeBetweenPruning,
		logger:               logger,
	}
	m.OnTerminating(func(_ error) { m.bundler.inProcess.Wait() }) // finish bundles that may be merging in parallel

	return m
}

func (m *Merger) Run() {
	m.logger.Info("starting merger")

	if err := m.startGRPCServer(); err != nil {
		m.logger.Error("cannot start GRPC server", zap.Error(err))
		m.Shutdown(err)
		return
	}

	m.startOldFilesPruner()

	err := m.run()
	if err != nil {
		m.logger.Error("merger returned error", zap.Error(err))
	}
	m.Shutdown(err)
}

func (m *Merger) startOldFilesPruner() {
	m.logger.Info("starting pruning of old files (delayed by time_between_pruning)",
		zap.Uint64("pruning_distance_to_head", m.pruningDistanceToLIB),
		zap.Duration("time_between_pruning", m.timeBetweenPruning),
	)

	go func() {
		delay := m.timeBetweenPruning // do not start pruning immediately
		for {
			time.Sleep(delay)
			now := time.Now()
			ctx := context.Background()

			var toDelete []*bstream.OneBlockFile

			pruningTarget := m.pruningTarget()
			m.io.WalkOneBlockFiles(ctx, m.firstStreamableBlock, func(obf *bstream.OneBlockFile) error {
				if obf.Num < pruningTarget {
					toDelete = append(toDelete, obf)
				}
				return nil
			})
			m.io.DeleteAsync(toDelete)

			if spentTime := time.Since(now); spentTime < m.timeBetweenPruning {
				delay = m.timeBetweenPruning - spentTime
			}
		}
	}()
}

func (m *Merger) pruningTarget() uint64 {
	bundlerBase := m.bundler.BaseBlockNum()
	if m.pruningDistanceToLIB > bundlerBase {
		return 0
	}

	return bundlerBase - m.pruningDistanceToLIB
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
		if m.bundler.stopBlock != 0 && base > m.bundler.stopBlock {
			if err == ErrStopBlockReached {
				m.logger.Info("stop block reached")
				return nil
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

		err = m.io.WalkOneBlockFiles(ctx, m.bundler.baseBlockNum, func(obf *bstream.OneBlockFile) error {
			m.logger.Debug("processing block", zap.Stringer("obf", obf))
			return m.bundler.HandleBlockFile(obf)
		})
		if err != nil {
			if err == ErrStopBlockReached {
				m.logger.Info("stop block reached")
				return nil
			}
			return err
		}

		if spentTime := time.Since(now); spentTime < m.timeBetweenPolling {
			time.Sleep(m.timeBetweenPolling - spentTime)
		}
	}
}
