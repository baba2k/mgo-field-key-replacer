package mongo

import (
	"context"
	"time"

	"github.com/baba2k/mgo-field-key-replacer/replace"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type MongoDB interface {
	ReplaceKeysInDocument(collection string, replaceMap map[string]string) (int64, int64, error)
}

type service struct {
	databaseName string
	client       *mongo.Client
}

func NewMongoDB(uri, databaseName string) (MongoDB, error) {
	// connect
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	// test connection
	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return nil, err
	}

	return &service{
		databaseName: databaseName,
		client:       client,
	}, err
}

func (s *service) ReplaceKeysInDocument(collection string, replaceMap map[string]string) (int64, int64, error) {
	var modifiedCountDoc int64
	var modifiedCountKey int64
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Minute)
	session, err := s.client.StartSession()
	if err != nil {
		return modifiedCountKey, modifiedCountDoc, err
	}
	defer session.EndSession(ctx)
	if err = session.StartTransaction(); err != nil {
		return modifiedCountKey, modifiedCountDoc, err
	}

	c := session.Client().Database(s.databaseName).Collection(collection)
	cur, err := c.Find(ctx, bson.M{})
	if err != nil {
		return modifiedCountKey, modifiedCountDoc, err
	}
	defer cur.Close(ctx)

	for cur.Next(ctx) {
		var result map[string]interface{}
		err := cur.Decode(&result)
		if err != nil {
			return modifiedCountKey, modifiedCountDoc, err
		}

		newMap, count := replace.ReplaceFieldKeysInDocument(result, replaceMap)
		if count > 0 {
			res, err := c.ReplaceOne(ctx, bson.M{"_id": result["_id"]}, newMap)
			if err != nil {
				return modifiedCountKey, modifiedCountDoc, err
			}
			modifiedCountDoc += res.ModifiedCount
			modifiedCountKey += count

		}
	}
	return modifiedCountKey, modifiedCountDoc, err
}
