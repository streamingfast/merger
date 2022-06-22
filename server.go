package merger

import (
	"bytes"
	"fmt"
	"net"
	"strings"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dgrpc"
	pbmerge "github.com/streamingfast/pbgo/sf/merger/v1"
	"go.uber.org/zap"
	pbhealth "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

func (m *Merger) startGRPCServer() error {
	gs := dgrpc.NewGRPCServer()
	m.logger.Info("grpc server created")

	lis, err := net.Listen("tcp", m.grpcListenAddr)
	if err != nil {
		return fmt.Errorf("failed listening grpc %q: %w", m.grpcListenAddr, err)
	}
	m.logger.Info("tcp listener created")
	m.OnTerminated(func(_ error) {
		lis.Close()
	})
	pbmerge.RegisterMergerServer(gs, m)
	pbhealth.RegisterHealthServer(gs, m)
	m.logger.Info("server registered")

	go func() {
		m.logger.Info("listening & serving grpc content", zap.String("grpc_listen_addr", m.grpcListenAddr))
		if err := gs.Serve(lis); err != nil {
			m.Shutdown(fmt.Errorf("error on grpc serve: %w", err))
			return
		}
	}()
	return nil
}

func (m *Merger) PreMergedBlocks(req *pbmerge.Request, server pbmerge.Merger_PreMergedBlocksServer) error {

	lowestNum := m.bundler.BaseBlockNum()
	if req.LowBlockNum < lowestNum || req.LowBlockNum >= lowestNum+m.bundler.bundleSize {
		err := fmt.Errorf("cannot find requested blocks")
		_ = server.SetHeader(metadata.New(map[string]string{"error": err.Error()}))
		return err
	}

	irreversibleBlocks := m.bundler.PreMergedBlocks()

	var foundHighBlockID bool
	var foundLowBlockNum bool
	for _, oneBlock := range irreversibleBlocks {
		if oneBlock.Num == req.LowBlockNum {
			foundLowBlockNum = true
		}
		if strings.HasSuffix(req.HighBlockID, oneBlock.ID) {
			foundHighBlockID = true
			break
		}
	}
	if !foundLowBlockNum {
		err := fmt.Errorf("cannot find requested lowBlockNum")
		server.SetHeader(metadata.New(map[string]string{"error": err.Error()}))
		return err
	}
	if !foundHighBlockID {
		err := fmt.Errorf("cannot find requested highBlockID")
		server.SetHeader(metadata.New(map[string]string{"error": err.Error()}))
		return err
	}

	for _, oneBlock := range irreversibleBlocks {
		if oneBlock.Num < req.LowBlockNum {
			continue
		}

		data, err := oneBlock.Data(server.Context(), m.io.DownloadOneBlockFile)
		if err != nil {
			return fmt.Errorf("unable to get one block data: %w", err)
		}

		blockReader, err := bstream.GetBlockReaderFactory.New(bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("unable to read one block: %w", err)
		}

		block, err := blockReader.Read()
		if block == nil {
			return err
		}

		protoBlock, err := block.ToProto()
		if protoBlock == nil || err != nil {
			return err
		}

		err = server.Send(
			&pbmerge.Response{
				Found: true,
				Block: protoBlock,
			})

		if err != nil {
			return fmt.Errorf("unable send response to client: %w", err)
		}
	}

	return nil
}
