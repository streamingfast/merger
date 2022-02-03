package merger

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	pbhealth "github.com/streamingfast/pbgo/grpc/health/v1"

	"github.com/streamingfast/merger/bundle"
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
