module github.com/streamingfast/merger

go 1.15

require (
	github.com/golang/protobuf v1.5.2
	github.com/streamingfast/bstream v0.0.2-0.20220113004316-0255c8f89fe3
	github.com/streamingfast/dbin v0.0.0-20210809205249-73d5eca35dc5
	github.com/streamingfast/dgrpc v0.0.0-20210901144702-c57c3701768b
	github.com/streamingfast/dmetrics v0.0.0-20210811180524-8494aeb34447
	github.com/streamingfast/dstore v0.1.1-0.20211012134319-16e840827e38
	github.com/streamingfast/logging v0.0.0-20210908162127-bdc5856d5341
	github.com/streamingfast/pbgo v0.0.6-0.20211209212750-753f0acb6553
	github.com/streamingfast/shutter v1.5.0
	github.com/stretchr/testify v1.7.0
	go.uber.org/zap v1.19.1
	google.golang.org/grpc v1.39.1
	gopkg.in/olivere/elastic.v3 v3.0.75
)

replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999
