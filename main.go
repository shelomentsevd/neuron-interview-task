package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/labstack/echo"
	"github.com/labstack/gommon/log"
	"github.com/shelomentsevd/neuron-interview-task/StreamStorage"
)

const (
	contentType        = "Content-Type"
	contentTypeJSONAPI = "application/vnd.api+json"
)

type jsonAPIError struct {
	Title  string `json:"title"`
	Detail string `json:"detail"`
}

type requestHandler struct {
	streamStorage StreamStorage.StreamStorage
}

func newRequestHandler(timeout time.Duration) requestHandler {
	return requestHandler{
		streamStorage: StreamStorage.NewStreamStorage(timeout),
	}
}

func (rh *requestHandler) createStream(c echo.Context) error {
	content := c.Request().Header.Get(contentType)
	if content != contentTypeJSONAPI {
		return c.NoContent(http.StatusUnsupportedMediaType)
	}
	body := c.Request().Body
	defer body.Close()

	// Read body
	bytes, err := ioutil.ReadAll(body)
	if err != nil {
		log.Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	// Decode json to Stream object
	var stream StreamStorage.Stream
	err = json.Unmarshal(bytes, stream)
	if err != nil {
		log.Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	if stream.Data.Type != StreamStorage.StreamType {
		return c.JSON(http.StatusBadRequest, jsonAPIError{
			Title:  "Wron type",
			Detail: fmt.Sprintf("Type %s is not accepted", stream.Data.Type),
		})
	}

	stream, err = rh.streamStorage.Create()
	if err != nil {
		log.Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	c.Response().Header().Add("Location", "/api/stream/"+stream.Data.ID)
	c.Response().Header().Add(contentType, contentTypeJSONAPI)
	return c.JSON(http.StatusCreated, stream)
}

func (rh *requestHandler) getList(c echo.Context) error {
	streams, err := rh.streamStorage.List()
	if err != nil {
		log.Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	c.Response().Header().Add(contentType, contentTypeJSONAPI)
	return c.JSON(http.StatusOK, streams)
}

func (rh *requestHandler) getStream(c echo.Context) error {
	ID := c.Param("ID")
	stream, err := rh.streamStorage.Get(ID)

	switch err {
	case StreamStorage.ErrStreamNotFound:
		return c.JSON(http.StatusNotFound, jsonAPIError{
			Title:  StreamStorage.ErrStreamNotFound.Error(),
			Detail: fmt.Sprintf("Stream with ID: %s not found", ID),
		})
	case nil:
	default:
		log.Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	c.Response().Header().Add(contentType, contentTypeJSONAPI)
	return c.JSON(http.StatusOK, stream)
}

func (rh *requestHandler) changeStream(c echo.Context) error {
	content := c.Request().Header.Get(contentType)
	if content != contentTypeJSONAPI {
		return c.NoContent(http.StatusUnsupportedMediaType)
	}

	ID := c.Param("ID")
	stream, err := rh.streamStorage.Get(ID)

	switch err {
	case StreamStorage.ErrStreamNotFound:
		return c.JSON(http.StatusNotFound, jsonAPIError{
			Title:  StreamStorage.ErrStreamNotFound.Error(),
			Detail: fmt.Sprintf("Stream with ID: %s not found", ID),
		})
	case nil:
	default:
		log.Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	body := c.Request().Body
	defer body.Close()

	// Read body
	bytes, err := ioutil.ReadAll(body)
	if err != nil {
		log.Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	var patch StreamStorage.Stream
	if err := json.Unmarshal(bytes, patch); err != nil {
		log.Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	if patch.Data.ID != ID || patch.Data.Type != StreamStorage.StreamType {
		return c.JSON(http.StatusBadRequest, jsonAPIError{
			Title:  "id or type is missing",
			Detail: "Every resource object MUST contain an id member and a type member.",
		})
	}

	switch patch.Data.Attributes.State {
	case StreamStorage.Active:
		err = rh.streamStorage.Run(ID)
	case StreamStorage.Finished:
		err = rh.streamStorage.Finish(ID)
	case StreamStorage.Interrupted:
		err = rh.streamStorage.Stop(ID)
	default:
		return c.JSON(http.StatusBadRequest, jsonAPIError{
			Title:  "Wrong state for update",
			Detail: fmt.Sprintf("%s is a wrong state for update", patch.Data.Attributes.State),
		})
	}

	switch err {
	case StreamStorage.ErrStreamWrongStateSwitch:
		return c.JSON(http.StatusForbidden, jsonAPIError{
			Title:  err.Error(),
			Detail: fmt.Sprintf("Stream %s can't switch to state %s", ID, patch.Data.Attributes.State),
		})
	case StreamStorage.ErrStreamNotFound:
		return c.NoContent(http.StatusNotFound)
	case StreamStorage.ErrStreamUnknowState:
		return c.JSON(http.StatusForbidden, jsonAPIError{
			Title:  err.Error(),
			Detail: fmt.Sprintf("Stream %s can't switch to unknow state %s", ID, patch.Data.Attributes.State),
		})
	case nil:
	default:
		log.Error(err)
		return c.NoContent(http.StatusInternalServerError)

	}

	c.Response().Header().Add(contentType, contentTypeJSONAPI)
	return c.JSON(http.StatusOK, stream)
}

func main() {
	var port string
	var timeout int
	flag.StringVar(&port, "port", "", "Server port")
	flag.IntVar(&timeout, "timeout", -1, "Stream interrupted state timeout in seconds")
	flag.Parse()

	if timeout < 0 {
		env := os.Getenv("TIMEOUT")
		var err error
		timeout, err = strconv.Atoi(env)
		if err != nil || timeout < 0 {
			log.Fatal("Invalid timeout. You can specify timeout through enviroment variable TIMEOUT or through flag -timeout.")
		}
	}

	if port == "" {
		port = os.Getenv("PORT")
	}

	if _, err := strconv.Atoi(port); err != nil {
		log.Fatal("Invalid port number. You can specify port through enviroment variable PORT or through flag -port.")
	}

	handler := newRequestHandler(time.Second * time.Duration(timeout))
	// Setup
	e := echo.New()
	e.Logger.SetLevel(log.INFO)

	// TODO: OPTIONS
	e.GET("/api/stream", handler.getList)
	e.GET("/api/stream/:ID", handler.getStream)
	e.POST("/api/stream", handler.createStream)
	e.PATCH("/api/stream/:ID", handler.changeStream)

	// Start server
	go func() {
		if err := e.Start(fmt.Sprintf(":%s", port)); err != nil {
			e.Logger.Info("shutting down the server")
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server with
	// a timeout of 10 seconds.
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.Shutdown(ctx); err != nil {
		e.Logger.Fatal(err)
	}
}
