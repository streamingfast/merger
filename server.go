package merger

import (
	"fmt"
	"net"

	pbmerge "github.com/dfuse-io/pbgo/dfuse/merger/v1"
	pbhealth "github.com/dfuse-io/pbgo/grpc/health/v1"
	"github.com/dfuse-io/dgrpc"
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
