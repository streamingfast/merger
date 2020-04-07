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

package main

import (
	"fmt"
	_ "net/http/pprof"
	"time"

	"github.com/abourget/viperbind"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	_ "github.com/dfuse-io/pbgo/dfuse/codecs/deth"
	_ "github.com/dfuse-io/bstream/codecs/deos"
	"github.com/dfuse-io/derr"
	mergerapp "github.com/dfuse-io/merger/app/merger"
	"github.com/dfuse-io/merger/metrics"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var rootCmd = &cobra.Command{Use: "merger", Short: "Operate the merger", RunE: mergerRunE}

func main() {
	cobra.OnInitialize(func() {
		viperbind.AutoBind(rootCmd, "MERGER")
	})

	rootCmd.PersistentFlags().String("storage-path-source", "gs://eoscanada-public-nodeos-archive/dev", "URL of storage to read one-block-files from")
	rootCmd.PersistentFlags().String("storage-path-dest", "gs://eoscanada-public-nodeos-archive/dev-merged", "URL of storage to write 100-block-files to")
	rootCmd.PersistentFlags().Duration("store-timeout", 2*time.Minute, "max time to to allow for each store operation")
	rootCmd.PersistentFlags().Duration("time-between-store-lookups", 10*time.Second, "delay between polling source store (higher for remote storage)")
	rootCmd.PersistentFlags().String("grpc-listen-addr", ":9001", "gRPC listen address to serve merger endpoints")
	rootCmd.PersistentFlags().String("protocol", "Protocol in string, must fit with bstream.ProtocolRegistry", "")
	rootCmd.PersistentFlags().Bool("process-live-blocks", true, "Ignore --start-.. and --stop-.. blocks, and process only live blocks")
	rootCmd.PersistentFlags().Uint64("start-block-num", 0, "FOR REPROCESSING: if >= 0, Set the block number where we should start processing")
	rootCmd.PersistentFlags().Uint64("stop-block-num", 0, "FOR REPROCESSING: if > 0, Set the block number where we should stop processing (and stop the process)")
	rootCmd.PersistentFlags().String("progress-filename", "", "FOR REPROCESSING: If non-empty, will update progress in this file and start right there on restart")
	rootCmd.PersistentFlags().Uint64("minimal-block-num", 0, "FOR LIVE: Set the minimal block number where we should start looking at the destination storage to figure out where to start")
	rootCmd.PersistentFlags().Duration("writers-leeway", time.Second*25, "how long we wait after seeing the upper boundary, to ensure that we get as many blocks as possible in a bundle")
	rootCmd.PersistentFlags().String("seen-blocks-file", "/tmp/merger.seen.gob", "file to save to / load from the map of 'seen blocks'")
	rootCmd.PersistentFlags().Uint64("max-fixable-fork", 1000, "after that number of blocks, a block belonging to another fork will be discarded (DELETED depending on flagDeleteBlocksBefore) instead of being inserted in last bundle")
	rootCmd.PersistentFlags().Bool("delete-blocks-before", false, "Enable deletion of one-block files when prior to the currently processed bundle (to avoid long file listings)")

	derr.Check("running merger", rootCmd.Execute())
}

func mergerRunE(cmd *cobra.Command, args []string) (err error) {
	setup()

	go metrics.ServeMetrics()

	protocolString := viper.GetString("global-protocol")
	protocol := pbbstream.Protocol(pbbstream.Protocol_value[protocolString])
	if protocol == pbbstream.Protocol_UNKNOWN {
		derr.Check("invalid protocol", fmt.Errorf("protocol value: %q", protocolString))
	}

	app := mergerapp.New(&mergerapp.Config{
		StoragePathSource:       viper.GetString("global-storage-path-source"),
		StoragePathDest:         viper.GetString("global-storage-path-dest"),
		StoreOperationTimeout:   viper.GetDuration("global-store-timeout"),
		TimeBetweenStoreLookups: viper.GetDuration("global-time-between-store-lookups"),
		GRPCListenAddr:          viper.GetString("global-grpc-listen-addr"),
		Protocol:                protocol,
		Live:                    viper.GetBool("global-process-live-blocks"),
		StartBlockNum:           viper.GetUint64("global-start-block-num"),
		StopBlockNum:            viper.GetUint64("global-stop-block-num"),
		ProgressFilename:        viper.GetString("global-progress-filename"),
		MinimalBlockNum:         viper.GetUint64("global-minimal-block-num"),
		WritersLeewayDuration:   viper.GetDuration("global-writers-leeway"),
		SeenBlocksFile:          viper.GetString("global-seen-blocks-file"),
		MaxFixableFork:          viper.GetUint64("global-max-fixable-fork"),
		DeleteBlocksBefore:      viper.GetBool("global-delete-blocks-before"),
	})
	derr.Check("merger app run", app.Run())

	select {
	case <-app.Terminated():
		if err = app.Err(); err != nil {
			zlog.Error("router shutdown with error", zap.Error(err))
		}
	case sig := <-derr.SetupSignalHandler(viper.GetDuration("global-shutdown-drain-delay")):
		zlog.Info("terminating through system signal", zap.Reflect("sig", sig))
		app.Shutdown(nil)
	}

	return

}
