#!/usr/bin/env bash

go build -buildmode=plugin ../mrapps/indexer.go
rm mr-out*
go run mrcoordinator.go pg*.txt
