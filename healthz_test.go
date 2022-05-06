package merger

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pbhealth "google.golang.org/grpc/health/grpc_health_v1"
)

func TestHealthz_Check(t *testing.T) {
	ctx := context.Background()
	bundler := newBundler(0, 0, 5)
	m := NewMerger(testLogger, bundler, time.Second, 10, "6969", nil, nil)

	request := &pbhealth.HealthCheckRequest{}
	resp, err := m.Check(ctx, request)
	if err != nil {
		panic(err)
	}

	require.Equal(t, resp.Status, pbhealth.HealthCheckResponse_SERVING)
}
