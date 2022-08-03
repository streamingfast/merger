package merger

import (
	"fmt"
	"net"

	"github.com/streamingfast/dgrpc"
	"go.uber.org/zap"
	pbhealth "google.golang.org/grpc/health/grpc_health_v1"
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
