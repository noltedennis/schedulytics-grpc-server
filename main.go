package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"

	"github.com/noltedennis/schedulytics-backend/model"
	"github.com/noltedennis/schedulytics-backend/services"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
)

// Global variables for db connection , collection and context
var db *mongo.Client
var jobdb *mongo.Collection
var mongoCtx context.Context

func main() {
	// Configure 'log' package to give file name and line number on eg. log.Fatal
	// Pipe flags to one another (log.LstdFLags = log.Ldate | log.Ltime)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Read in ENV values
	pw, ok := os.LookupEnv("MONGO_PW")
	if !ok {
		fmt.Println("error: unable to find MONGO_PW in the environment")
		os.Exit(1)
	}

	// Initialize MongoDb client
	fmt.Println("Connecting to MongoDB...")
	mongoURI := fmt.Sprintf("mongodb://%s:%s@%s", "schedulytics", pw, "mongodb:27017/schedulytics")
	fmt.Println("connection string is:", mongoURI)

	// non-nil empty context
	mongoCtx = context.Background()

	// Connect takes in a context and options, the connection URI is the only option we pass for now
	db, err := mongo.Connect(mongoCtx, options.Client().ApplyURI(mongoURI))
	// Handle potential errors
	if err != nil {
		log.Fatal(err)
	}

	// Check whether the connection was succesful by pinging the MongoDB server
	err = db.Ping(mongoCtx, nil)
	if err != nil {
		log.Fatalf("Could not connect to MongoDB: %v\n", err)
	} else {
		fmt.Println("Connected to Mongodb")
	}

	// Bind our collection to our global variable for use in other methods
	jobdb := db.Database("schedulytics").Collection("job")

	// Start to listen on port 8010
	fmt.Println("Starting server on port :8010...")
	path := "0.0.0.0:8010"
	lis, err := net.Listen("tcp", path)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("Listening on %s", path)

	// Set options, here we can configure things like TLS support
	opts := []grpc.ServerOption{}
	// Create new gRPC server with (blank) options
	s := grpc.NewServer(opts...)

	// Create JobService type
	jobSrv := &services.JobServiceServer{
		JobDb:    jobdb,
		MongoCtx: mongoCtx,
	}
	// Register the service with the server
	model.RegisterJobServiceServer(s, jobSrv)

	// Same for the HelloService
	helloSrv := &services.HelloServiceServer{}
	model.RegisterHelloServiceServer(s, helloSrv)

	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()
	fmt.Println("Server succesfully started on port :8010")

	// Right way to stop the server using a SHUTDOWN HOOK
	// Create a channel to receive OS signals
	c := make(chan os.Signal)

	// Relay os.Interrupt to our channel (os.Interrupt = CTRL+C)
	// Ignore other incoming signals
	signal.Notify(c, os.Interrupt)

	// Block main routine until a signal is received
	// As long as user doesn't press CTRL+C a message is not passed and our main routine keeps running
	<-c

	// After receiving CTRL+C Properly stop the server
	fmt.Println("\nStopping the server...")
	s.Stop()
	lis.Close()
	fmt.Println("Closing MongoDB connection")
	db.Disconnect(mongoCtx)
	fmt.Println("Done.")
}
