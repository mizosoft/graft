package main

import "google.golang.org/protobuf/proto"

func protoMarshal(msg proto.Message) []byte {
	data, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return data
}
