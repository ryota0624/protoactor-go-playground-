proto:
	cd proto && buf generate

run_remote:
	PORT=8889 go run ./bin/remote/main.go
	PORT=9000 go run ./bin/remote/main.go
