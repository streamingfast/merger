module github.com/dfuse-io/merger

require (
	cloud.google.com/go/storage v1.4.0
	github.com/abourget/llerrgroup v0.2.0
	github.com/dfuse-io/bstream v0.0.0-20200427161155-5bc64e86c005
	github.com/dfuse-io/dbin v0.0.0-20200406215642-ec7f22e794eb
	github.com/dfuse-io/derr v0.0.0-20200406214256-c690655246a1
	github.com/dfuse-io/dgrpc v0.0.0-20200406214416-6271093e544c
	github.com/dfuse-io/dmetrics v0.0.0-20200406214800-499fc7b320ab
	github.com/dfuse-io/dstore v0.1.0
	github.com/dfuse-io/logging v0.0.0-20200407175011-14021b7a79af
	github.com/dfuse-io/pbgo v0.0.6-0.20200602201455-99986ef5a09d
	github.com/dfuse-io/shutter v1.4.1-0.20200319040708-c809eec458e6
	github.com/golang/protobuf v1.4.2 // indirect
	github.com/stretchr/testify v1.4.0
	go.uber.org/zap v1.14.0
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e
	gopkg.in/olivere/elastic.v3 v3.0.75
	gopkg.in/yaml.v2 v2.2.4 // indirect
)

replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999

go 1.13
