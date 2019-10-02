# FluentBit AWS-SQS Output Plugin

FluntBit custom output plugin which allows sending messages to AWS-SQS.

## Configuration Parameters

| Configuration Key Name  | Description                          | Mandatory |
|-------------------------|--------------------------------------|-----------|
| QueueUrl                | the queue url in your aws account    | yes       |
| QueueRegion             | the queue region in your aws account | yes       |
| PluginTagAttribute      | attribute name of the message tag    | no        | 

```conf
[SERVICE]
    Flush        5
    Daemon       Off
    Log_Level    info

    HTTP_Server  On
    HTTP_Listen  0.0.0.0
    HTTP_Port    2020

[INPUT]
    Name   dummy
    Rate   1
    Tag    dummy.log

[OUTPUT]
    Name  sqs
    Match *
    QueueUrl    http://aws-sqs-url.com
    QueueRegion eu-central-1
```

## Installation

Example of installation in docker file:  

```bash
FROM golang:1.12 as gobuilder

WORKDIR /root

ENV GOOS=linux\
    GOARCH=amd64

COPY / /root/

RUN go build \
    -buildmode=c-shared \
    -o /out_sqs.so \
    github.com/PayU/fluentBit-sqs-plugin

FROM fluent/fluent-bit:1.1

COPY --from=gobuilder /out_sqs.so /fluent-bit/bin/

EXPOSE 2020

ENTRYPOINT ["/fluent-bit/bin/fluent-bit"]
CMD ["-c", "/fluent-bit/etc/some_configuration.conf", "-e", "/fluent-bit/bin/out_sqs.so"]
```

More information about the usage and installation of golang plugins can be found here: https://docs.fluentbit.io/manual/development/golang_plugins 

## Special Notes

- Aws Sqs credentials in golang SDK: </br> When you initialize a new service client without providing any credential arguments, the SDK uses the default credential provider chain to find AWS credentials. The SDK uses the first provider in the chain that returns credentials without an error. The default provider chain looks for credentials in the following order:

    	1) Environment variables. (AWS_SECRET_ACCESS_KEY and AWS_SECRET_KEY)

    	2)Shared credentials file.

		3) If your application is running on an Amazon EC2 instance, IAM role for Amazon EC2.
