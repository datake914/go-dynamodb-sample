package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// Item is record for MyFirstTable.
type Item struct {
	MyHashKey  string `dynamodbav:"MyHashKey"`
	MyRangeKey int    `dynamodbav:"MyRangeKey"`
	MyText     string `dynamodbav:"MyText"`
}

func main() {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	db := dynamodb.New(sess, &aws.Config{Endpoint: aws.String("http://localhost:8000")})

	put(db)
	get(db)
	update(db)
	delete(db)
}

func put(db *dynamodb.DynamoDB) {
	item := Item{
		MyHashKey:  "00001",
		MyRangeKey: 1,
		MyText:     "some text...",
	}
	// Convert item to dynamodb attribute.
	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	// Create input.
	input := &dynamodb.PutItemInput{
		TableName: aws.String("MyFirstTable"),
		Item:      av,
	}
	// Execute.
	_, err = db.PutItem(input)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
}

func get(db *dynamodb.DynamoDB) {
	// Create input.
	input := &dynamodb.GetItemInput{
		TableName: aws.String("MyFirstTable"),
		Key: map[string]*dynamodb.AttributeValue{
			"MyHashKey": {
				S: aws.String("00001"),
			},
			"MyRangeKey": {
				N: aws.String("1"),
			},
		},
	}
	// Execute.
	result, err := db.GetItem(input)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	// Convert the dynamodb result to a struct.
	item := Item{}
	err = dynamodbattribute.UnmarshalMap(result.Item, &item)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println(item) // {00001 1 some text...}
}

func update(db *dynamodb.DynamoDB) {
	// Create an expression for update.
	update := expression.UpdateBuilder{}.Set(expression.Name("MyText"), expression.Value("updated text"))
	expr, err := expression.NewBuilder().WithUpdate(update).Build()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	// Create an input.
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String("MyFirstTable"),
		Key: map[string]*dynamodb.AttributeValue{
			"MyHashKey": {
				S: aws.String("00001"),
			},
			"MyRangeKey": {
				N: aws.String("1"),
			},
		},
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		ReturnValues:              aws.String(dynamodb.ReturnValueAllNew),
	}
	// Execute.
	_, err = db.UpdateItem(input)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
}

func delete(db *dynamodb.DynamoDB) {
	// Create an input.
	input := &dynamodb.DeleteItemInput{
		TableName: aws.String("MyFirstTable"),
		Key: map[string]*dynamodb.AttributeValue{
			"MyHashKey": {
				S: aws.String("00001"),
			},
			"MyRangeKey": {
				N: aws.String("1"),
			},
		},
	}
	// Execute.
	_, err := db.DeleteItem(input)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
}
