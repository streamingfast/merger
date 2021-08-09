module github.com/streamingfast/merger

go 1.15

require (
	cloud.google.com/go/storage v1.4.0
	github.com/abourget/llerrgroup v0.2.0
	github.com/dfuse-io/bstream v0.0.2-0.20200724152201-64aae5bc532f
	github.com/dfuse-io/dbin v0.0.0-20200406215642-ec7f22e794eb
	github.com/dfuse-io/dgrpc v0.0.0-20200406214416-6271093e544c
	github.com/dfuse-io/dmetrics v0.0.0-20200406214800-499fc7b320ab
	github.com/dfuse-io/dstore v0.1.1-0.20200820172424-8e0e519ece2f
	github.com/dfuse-io/logging v0.0.0-20200407175011-14021b7a79af
	github.com/dfuse-io/pbgo v0.0.6-0.20200722182828-c2634161d5a3
	github.com/dfuse-io/shutter v1.4.1-0.20200319040708-c809eec458e6
	github.com/stretchr/testify v1.4.0
	go.uber.org/zap v1.14.0
	google.golang.org/grpc v1.26.0
	gopkg.in/olivere/elastic.v3 v3.0.75
	gopkg.in/yaml.v2 v2.2.4 // indirect
)

replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999
