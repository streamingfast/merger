package merger

import (
	"fmt"
	"net"

	"github.com/streamingfast/dgrpc"
	pbmerge "github.com/streamingfast/pbgo/dfuse/merger/v1"
	pbhealth "github.com/streamingfast/pbgo/grpc/health/v1"
	"go.uber.org/zap"
)

func (m *Merger) startServer() {
	gs := dgrpc.NewServer()
	zlog.Info("grpc server created")

	lis, err := net.Listen("tcp", m.grpcListenAddr)
	if err != nil {
		m.Shutdown(fmt.Errorf("failed listening grpc %q: %w", m.grpcListenAddr, err))
		return
	}
	zlog.Info("tcp listener created")

	pbmerge.RegisterMergerServer(gs, m)
	pbhealth.RegisterHealthServer(gs, m)
	zlog.Info("server registered")

	go func() {
		zlog.Info("listening & serving grpc content", zap.String("grpc_listen_addr", m.grpcListenAddr))
		if err := gs.Serve(lis); err != nil {
			m.Shutdown(fmt.Errorf("error on grpc serve: %w", err))
			return
		}
	}()
}

func (m *Merger) PreMergedBlocks(req *pbmerge.Request, server pbmerge.Merger_PreMergedBlocksServer) error {

	//
	//if req.LowBlockNum < m.bundle.lowerBlock || req.LowBlockNum >= m.bundle.upperBlock() {
	//	err := fmt.Errorf("cannot find requested blocks")
	//	server.SetHeader(metadata.New(map[string]string{"error": err.Error()}))
	//	return err
	//}
	//
	//files := m.bundle.timeSortedFiles()
	//var foundHighBlockID bool
	//var foundLowBlockNum bool
	//for _, oneBlock := range files {
	//	if oneBlock.num == req.LowBlockNum {
	//		foundLowBlockNum = true
	//	}
	//	if strings.HasSuffix(req.HighBlockID, oneBlock.id) {
	//		foundHighBlockID = true
	//		break
	//	}
	//}
	//if !foundLowBlockNum {
	//	err := fmt.Errorf("cannot find requested lowBlockNum")
	//	server.SetHeader(metadata.New(map[string]string{"error": err.Error()}))
	//	return err
	//}
	//if !foundHighBlockID {
	//	err := fmt.Errorf("cannot find requested highBlockID")
	//	server.SetHeader(metadata.New(map[string]string{"error": err.Error()}))
	//	return err
	//}
	//
	//for _, oneBlock := range m.bundle.timeSortedFiles() {
	//	if oneBlock.num < req.LowBlockNum {
	//		continue
	//	}
	//	data, err := oneBlock.Data(server.Context(), m.oneBlocksStore)
	//	if err != nil {
	//		return fmt.Errorf("unable to get one block data: %w", err)
	//	}
	//
	//	blockReader, err := bstream.GetBlockReaderFactory.New(bytes.NewReader(data))
	//	if err != nil {
	//		return fmt.Errorf("unable to read one block: %w", err)
	//	}
	//
	//	block, err := blockReader.Read()
	//	if block == nil {
	//		return err
	//	}
	//
	//	protoblock, err := block.ToProto()
	//	if protoblock == nil || err != nil {
	//		return err
	//	}
	//
	//	err = server.Send(
	//		&pbmerge.Response{
	//			Found: true, //todo: this is not require any more
	//			Block: protoblock,
	//		})
	//
	//	if err != nil {
	//		return fmt.Errorf("unable send response to client: %w", err)
	//	}
	//
	//	if strings.HasSuffix(req.HighBlockID, oneBlock.id) {
	//		break
	//	}
	//}
	//
	return nil
}
