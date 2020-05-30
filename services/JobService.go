package services

import (
	"context"
	"fmt"
	"log"

	"github.com/noltedennis/schedulytics-backend/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type JobItem struct {
	ID          primitive.ObjectID `bson:"_id,omitempty"`
	Name        string             `bson:"name"`
	Owner       string             `bson:"owner"`
	Description string             `bson:"description"`
}

type JobServiceServer struct {
	JobDb    *mongo.Collection
	MongoCtx context.Context
}

func newJobSever() *JobServiceServer {
	log.Printf("Registered JobServiceServer handler")
	return &JobServiceServer{}
}

func (s *JobServiceServer) CreateJob(ctx context.Context, req *model.CreateJobReq) (*model.CreateJobRes, error) {
	// Essentially doing req.Job to access the struct with a nil check
	Job := req.GetJob()
	// Now we have to convert this into a JobItem type to convert into BSON
	data := JobItem{
		// ID:    Empty, so it gets omitted and MongoDB generates a unique Object ID upon insertion.
		Name:        Job.GetName(),
		Owner:       Job.GetOwner(),
		Description: Job.GetDescription(),
	}

	// Insert the data into the database, result contains the newly generated Object ID for the new document
	result, err := s.JobDb.InsertOne(s.MongoCtx, data)
	// check for potential errors
	if err != nil {
		// return internal gRPC error to be handled later
		return nil, status.Errorf(
			codes.Internal,
			fmt.Sprintf("Internal error: %v", err),
		)
	}
	// add the id to Job, first cast the "generic type" (go doesn't have real generics yet) to an Object ID.
	oid := result.InsertedID.(primitive.ObjectID)
	// Convert the object id to it's string counterpart
	Job.Id = oid.Hex()
	// return the Job in a CreateJobRes type
	return &model.CreateJobRes{Job: Job}, nil
}

func (s *JobServiceServer) ReadJob(ctx context.Context, req *model.ReadJobReq) (*model.ReadJobRes, error) {
	// convert string id (from proto) to mongoDB ObjectId
	oid, err := primitive.ObjectIDFromHex(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, fmt.Sprintf("Could not convert to ObjectId: %v", err))
	}
	result := s.JobDb.FindOne(ctx, bson.M{"_id": oid})
	// Create an empty JobItem to write our decode result to
	data := JobItem{}
	// decode and write to data
	if err := result.Decode(&data); err != nil {
		return nil, status.Errorf(codes.NotFound, fmt.Sprintf("Could not find Job with Object Id %s: %v", req.GetId(), err))
	}
	// Cast to ReadJobRes type
	response := &model.ReadJobRes{
		Job: &model.Job{
			Id:          oid.Hex(),
			Name:        data.Name,
			Owner:       data.Owner,
			Description: data.Description,
		},
	}
	return response, nil
}

func (s *JobServiceServer) DeleteJob(ctx context.Context, req *model.DeleteJobReq) (*model.DeleteJobRes, error) {
	// Get the ID (string) from the request message and convert it to an Object ID
	oid, err := primitive.ObjectIDFromHex(req.GetId())
	// Check for errors
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, fmt.Sprintf("Could not convert to ObjectId: %v", err))
	}
	// DeleteOne returns DeleteResult which is a struct containing the amount of deleted docs (in this case only 1 always)
	// So we return a boolean instead
	_, err = s.JobDb.DeleteOne(ctx, bson.M{"_id": oid})
	// Check for errors
	if err != nil {
		return nil, status.Errorf(codes.NotFound, fmt.Sprintf("Could not find/delete Job with id %s: %v", req.GetId(), err))
	}
	// Return response with success: true if no error is thrown (and thus document is removed)
	return &model.DeleteJobRes{
		Success: true,
	}, nil
}

func (s *JobServiceServer) UpdateJob(ctx context.Context, req *model.UpdateJobReq) (*model.UpdateJobRes, error) {
	// Get the Job data from the request
	Job := req.GetJob()

	// Convert the Id string to a MongoDB ObjectId
	oid, err := primitive.ObjectIDFromHex(Job.GetId())
	if err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			fmt.Sprintf("Could not convert the supplied Job id to a MongoDB ObjectId: %v", err),
		)
	}

	// Convert the data to be updated into an unordered Bson document
	update := bson.M{
		"name":        Job.GetName(),
		"owner":       Job.GetOwner(),
		"description": Job.GetDescription(),
	}

	// Convert the oid into an unordered bson document to search by id
	filter := bson.M{"_id": oid}

	// Result is the BSON encoded result
	// To return the updated document instead of original we have to add options.
	result := s.JobDb.FindOneAndUpdate(ctx, filter, bson.M{"$set": update}, options.FindOneAndUpdate().SetReturnDocument(1))

	// Decode result and write it to 'decoded'
	decoded := JobItem{}
	err = result.Decode(&decoded)
	if err != nil {
		return nil, status.Errorf(
			codes.NotFound,
			fmt.Sprintf("Could not find Job with supplied ID: %v", err),
		)
	}
	return &model.UpdateJobRes{
		Job: &model.Job{
			Id:          decoded.ID.Hex(),
			Name:        decoded.Name,
			Owner:       decoded.Owner,
			Description: decoded.Description,
		},
	}, nil
}

func (s *JobServiceServer) ListJobs(req *model.ListJobsReq, stream model.JobService_ListJobsServer) error {
	// Initiate a JobItem type to write decoded data to
	data := &JobItem{}
	// collection.Find returns a cursor for our (empty) query
	cursor, err := s.JobDb.Find(context.Background(), bson.M{})
	if err != nil {
		return status.Errorf(codes.Internal, fmt.Sprintf("Unknown internal error: %v", err))
	}
	// An expression with defer will be called at the end of the function
	defer cursor.Close(context.Background())
	// cursor.Next() returns a boolean, if false there are no more items and loop will break
	for cursor.Next(context.Background()) {
		// Decode the data at the current pointer and write it to data
		err := cursor.Decode(data)
		// check error
		if err != nil {
			return status.Errorf(codes.Unavailable, fmt.Sprintf("Could not decode data: %v", err))
		}
		// If no error is found send Job over stream
		stream.Send(&model.ListJobsRes{
			Job: &model.Job{
				Id:          data.ID.Hex(),
				Name:        data.Name,
				Owner:       data.Owner,
				Description: data.Description,
			},
		})
	}
	// Check if the cursor has any errors
	if err := cursor.Err(); err != nil {
		return status.Errorf(codes.Internal, fmt.Sprintf("Unkown cursor error: %v", err))
	}
	return nil
}
