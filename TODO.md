# TODO

- [X] Implement Polars LazyFrame Engines
  - Support lazy evaluation for better memory efficiency
  - Add streaming capabilities
  - Implement optimized query planning

- [ ] Add Local Provider Support
  - File system operations
  - Local metadata storage
  - Testing environment setup

- [X] DuckDB SQL Engine Integration 
  - AWS Glue compatibility layer
  - Parquet/CSV native support
  - Performance optimizations for AWS
  - Query pushdown support
  - Copy of catalog metadata
  - Reference: [DuckDB on AWS Glue](https://cabeda.medium.com/running-duckdb-on-aws-glue-643b8a1e4deb)

- [ ] Documentation
  - Create comprehensive README.md
  - Add installation guide
  - Include usage examples
  - Document API reference
  - Add contributing guidelines

- [ ] Add new providers
  - Google Cloud Storage
  - Azure Blob Storage
  - Others...