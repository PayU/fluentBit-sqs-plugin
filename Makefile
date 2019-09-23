all:
	go build -buildmode=c-shared -o out_sqs.so .
	
fast:
	go build out_sqs.go

clean:
	rm -rf *.so *.h *~
