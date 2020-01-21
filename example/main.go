package main

import (
	"fmt"

	"github.com/baba2k/mgo-field-key-replacer/mongo"
)

func main() {
	uri := "mongodb://localhost:27017"
	database := "test"
	collection := "test"
	replaceMap := map[string]string{
		"test": "TEST",
	}

	mongoDb, err := mongo.NewMongoDB(uri, database)
	if err != nil {
		panic(err)
	}

	modifiedCountKey, modifiedCountDoc, err := mongoDb.ReplaceKeysInDocument(collection, replaceMap)
	if err != nil {
		panic(err)
	}

	fmt.Println(modifiedCountKey, "field keys in", modifiedCountDoc, "documents modified")
}
