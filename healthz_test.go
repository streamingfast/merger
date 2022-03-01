package merger

import (
	"context"
	"testing"
	"time"

	"github.com/streamingfast/merger/bundle"
	"github.com/stretchr/testify/require"
	pbhealth "google.golang.org/grpc/health/grpc_health_v1"
)

func TestHealthz_Check(t *testing.T) {
	ctx := context.Background()
	bundler := bundle.NewBundler(5, 5)
	m := NewMerger(bundler, time.Second, "6969", nil, nil, "")

	request := &pbhealth.HealthCheckRequest{}
	resp, err := m.Check(ctx, request)
	if err != nil {
		panic(err)
	}

	require.Equal(t, resp.Status, pbhealth.HealthCheckResponse_SERVING)
}
