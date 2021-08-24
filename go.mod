module github.com/streamingfast/merger

go 1.15

require (
	github.com/streamingfast/bstream v0.0.2-0.20210818191722-64ffe9f21879
	github.com/streamingfast/dbin v0.0.0-20210809205249-73d5eca35dc5
	github.com/streamingfast/dgrpc v0.0.0-20210811180351-8646818518b2
	github.com/streamingfast/dmetrics v0.0.0-20210811180524-8494aeb34447
	github.com/streamingfast/dstore v0.1.1-0.20210811180812-4db13e99cc22
	github.com/streamingfast/logging v0.0.0-20210811175431-f3b44b61606a
	github.com/streamingfast/pbgo v0.0.6-0.20210811160400-7c146c2db8cc
	github.com/streamingfast/shutter v1.5.0
	github.com/stretchr/testify v1.7.0
	go.uber.org/zap v1.15.0
	google.golang.org/grpc v1.39.1
	gopkg.in/olivere/elastic.v3 v3.0.75
)

replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999
replace github.com/streamingfast/bstream => /Users/cbillett/devel/sf/bstream