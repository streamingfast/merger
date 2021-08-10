module github.com/streamingfast/merger

go 1.15

require (
	github.com/dfuse-io/bstream v0.0.2-0.20210810200727-06a4c4aaeb99
	github.com/dfuse-io/logging v0.0.0-20210109005628-b97a57253f70
	github.com/dfuse-io/pbgo v0.0.6-0.20210429181308-d54fc7723ad3
	github.com/streamingfast/dbin v0.0.0-20210809205249-73d5eca35dc5
	github.com/streamingfast/dgrpc v0.0.0-20210810185305-905172f728e8
	github.com/streamingfast/dmetrics v0.0.0-20210810172555-564d623b116f
	github.com/streamingfast/dstore v0.1.1-0.20210810110932-928f221474e4
	github.com/streamingfast/shutter v1.5.0
	github.com/stretchr/testify v1.7.0
	go.uber.org/zap v1.15.0
	google.golang.org/grpc v1.29.1
	gopkg.in/olivere/elastic.v3 v3.0.75
)

replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999
