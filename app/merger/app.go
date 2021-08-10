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
	"time"

	"github.com/streamingfast/dmetrics"
	"github.com/streamingfast/merger/metrics"

	"github.com/streamingfast/dgrpc"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/merger"
	pbhealth "github.com/dfuse-io/pbgo/grpc/health/v1"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

type Config struct {
	StorageOneBlockFilesPath       string
	StorageMergedBlocksFilesPath   string
	GRPCListenAddr                 string
	BatchMode                      bool
	StartBlockNum                  uint64
	StopBlockNum                   uint64
	MinimalBlockNum                uint64
	WritersLeewayDuration          time.Duration
	TimeBetweenStoreLookups        time.Duration
	StateFile                      string
	MaxFixableFork                 uint64
	OneBlockDeletionThreads        int
	MaxOneBlockOperationsBatchSize int
}

type App struct {
	*shutter.Shutter
	config         *Config
	readinessProbe pbhealth.HealthClient
}

func New(config *Config) *App {
	return &App{
		Shutter: shutter.New(),
		config:  config,
	}
}

func (a *App) Run() error {
	zlog.Info("running merger", zap.Reflect("config", a.config))

	if a.config.OneBlockDeletionThreads < 1 {
		return fmt.Errorf("need at least 1 OneBlockDeletionThread")
	}
	if a.config.MaxOneBlockOperationsBatchSize < 250 {
		return fmt.Errorf("minimum MaxOneBlockOperationsBatchSize is 250")
	}

	dmetrics.Register(metrics.MetricSet)

	sourceArchiveStore, err := dstore.NewDBinStore(a.config.StorageOneBlockFilesPath)
	if err != nil {
		return fmt.Errorf("failed to init source archive store: %w", err)
	}

	mergedBlocksStore, err := dstore.NewDBinStore(a.config.StorageMergedBlocksFilesPath)
	if err != nil {
		return fmt.Errorf("failed to init destination archive store: %w", err)
	}

	var startBlockNum uint64
	var stopBlockNum uint64
	var seenBlocks *merger.SeenBlockCache

	if a.config.BatchMode {
		startBlockNum = a.config.StartBlockNum
		stopBlockNum = a.config.StopBlockNum
		seenBlocks = merger.NewSeenBlockCacheInMemory(startBlockNum, a.config.MaxFixableFork)
	} else {
		if a.config.StartBlockNum != 0 {
			seenBlocks = merger.NewSeenBlockCacheFromNewFile(a.config.StateFile, a.config.MaxFixableFork)
			startBlockNum = a.config.StartBlockNum
		} else {
			seenBlocks = merger.NewSeenBlockCacheFromFile(a.config.StateFile, a.config.MaxFixableFork)
			if seenBlocks.HighestBlockNum != 0 {
				startBlockNum = seenBlocks.HighestBlockNum + 1
			} else {
				startBlockNum, err = merger.FindNextBaseMergedBlock(mergedBlocksStore, a.config.MinimalBlockNum, 100)
				if err != nil {
					return fmt.Errorf("finding where to start: %w", err)
				}
			}
		}
	}

	m := merger.NewMerger(
		sourceArchiveStore,
		mergedBlocksStore,
		a.config.WritersLeewayDuration,
		100,
		seenBlocks,
		startBlockNum,
		stopBlockNum,
		a.config.TimeBetweenStoreLookups,
		a.config.GRPCListenAddr,
		a.config.OneBlockDeletionThreads,
		a.config.MaxOneBlockOperationsBatchSize,
		a.config.BatchMode,
	)
	zlog.Info("merger initiated")

	gs, err := dgrpc.NewInternalClient(a.config.GRPCListenAddr)
	if err != nil {
		return fmt.Errorf("cannot create readiness probe")
	}
	a.readinessProbe = pbhealth.NewHealthClient(gs)

	a.OnTerminating(m.Shutdown)
	m.OnTerminated(a.Shutdown)

	go m.Launch()

	zlog.Info("merger running")
	return nil
}

func (a *App) IsReady() bool {
	if a.readinessProbe == nil {
		return false
	}

	resp, err := a.readinessProbe.Check(context.Background(), &pbhealth.HealthCheckRequest{})
	if err != nil {
		zlog.Info("merger readiness probe error", zap.Error(err))
		return false
	}

	if resp.Status == pbhealth.HealthCheckResponse_SERVING {
		return true
	}

	return false
}
