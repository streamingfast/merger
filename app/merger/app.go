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
	"strconv"
	"time"

	"github.com/dfuse-io/dgrpc"
	"github.com/dfuse-io/dstore"
	"github.com/dfuse-io/merger"
	pbhealth "github.com/dfuse-io/pbgo/grpc/health/v1"
	"github.com/dfuse-io/shutter"
	"go.uber.org/zap"
)

type Config struct {
	StorageOneBlockFilesPath     string
	StorageMergedBlocksFilesPath string
	StoreOperationTimeout        time.Duration
	GRPCListenAddr               string
	Live                         bool
	StartBlockNum                uint64
	StopBlockNum                 uint64
	ProgressFilename             string
	MinimalBlockNum              uint64
	WritersLeewayDuration        time.Duration
	TimeBetweenStoreLookups      time.Duration
	SeenBlocksFile          string
	MaxFixableFork          uint64
	DeleteBlocksBefore      bool
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

	sourceArchiveStore, err := dstore.NewDBinStore(a.config.StorageOneBlockFilesPath)
	if err != nil {
		return fmt.Errorf("failed to init source archive store: %w", err)
	}
	sourceArchiveStore.SetOperationTimeout(a.config.StoreOperationTimeout)

	destArchiveStore, err := dstore.NewDBinStore(a.config.StorageMergedBlocksFilesPath)
	if err != nil {
		return fmt.Errorf("failed to init destination archive store: %w", err)
	}

	destArchiveStore.SetOperationTimeout(a.config.StoreOperationTimeout)

	m := merger.NewMerger(sourceArchiveStore, destArchiveStore, a.config.WritersLeewayDuration, a.config.MinimalBlockNum, a.config.ProgressFilename, a.config.DeleteBlocksBefore, a.config.SeenBlocksFile, a.config.TimeBetweenStoreLookups, a.config.MaxFixableFork, a.config.GRPCListenAddr)
	zlog.Info("merger initiated")

	var startBlockNum uint64
	var stopBlockNum uint64
	if a.config.Live {
		startBlockNum, err = m.FindNextBaseBlock()
		if err != nil {
			return fmt.Errorf("finding where to start: %w", err)
		}
	} else {
		startBlockNum = a.config.StartBlockNum
		if start, err := getStartBlockFromProgressFile(a.config.ProgressFilename); err == nil {
			if start > startBlockNum {
				startBlockNum = start
			}
		}
		stopBlockNum = a.config.StopBlockNum
	}

	m.SetupBundle(startBlockNum, stopBlockNum)

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

func getStartBlockFromProgressFile(filename string) (uint64, error) {
	if filename == "" {
		return 0, fmt.Errorf("filename not set")
	}
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return 0, err
	}
	val, err := strconv.ParseUint(fmt.Sprintf("%s", content), 10, 64)
	if err != nil {
		return 0, err
	}
	return val, nil
}
