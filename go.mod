module github.com/streamingfast/merger

go 1.15

require (
	cloud.google.com/go/storage v1.4.0
	github.com/abourget/llerrgroup v0.2.0
	github.com/dfuse-io/bstream v0.0.2-0.20210810055526-1c2b722f0cf6
	github.com/dfuse-io/dgrpc v0.0.0-20210128133958-db1ca95920e4
	github.com/dfuse-io/dmetrics v0.0.0-20200406214800-499fc7b320ab
	github.com/dfuse-io/dstore v0.1.1-0.20210507180120-88a95674809f
	github.com/dfuse-io/logging v0.0.0-20210109005628-b97a57253f70
	github.com/dfuse-io/pbgo v0.0.6-0.20210429181308-d54fc7723ad3
	github.com/dfuse-io/shutter v1.4.1
	github.com/streamingfast/dbin v0.0.0-20210809205249-73d5eca35dc5
	github.com/stretchr/testify v1.4.0
	go.uber.org/zap v1.15.0
	google.golang.org/grpc v1.29.1
	gopkg.in/olivere/elastic.v3 v3.0.75
	gopkg.in/yaml.v2 v2.2.4 // indirect
)

replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999
