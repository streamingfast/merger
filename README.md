# dfuse Merger

The merger process is responsible for accumulating blocks from all
forks visible by the pool of instrumented nodes, and builds the famoux
100-blocks files consumed by `bstream`'s _FileSource_ and may other
dfuse processes.

## Installation & Usage

See the different protocol-specific `dfuse` binaries at https://github.com/dfuse-io/dfuse#protocols

Current implementations:

* [**dfuse for EOSIO**](https://github.com/dfuse-io/dfuse-eosio)
* **dfuse for Ethereum**, soon to be open sourced


## Contributing

**Issues and PR in this repo related strictly to the merger functionalities**

Report any protocol-specific issues in their
[respective repositories](https://github.com/dfuse-io/dfuse#protocols)

**Please first refer to the general
[dfuse contribution guide](https://github.com/dfuse-io/dfuse#contributing)**,
if you wish to contribute to this code base.


## License

[Apache 2.0](LICENSE)
