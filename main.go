package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/labstack/echo"
	"github.com/labstack/gommon/log"
	"github.com/shelomentsevd/neuron-interview-task/StreamStorage"
)

type requestHandler struct {
	streamStorage StreamStorage.StreamStorage
}

func newRequestHandler(timeout time.Duration) requestHandler {
	return requestHandler{
		streamStorage: StreamStorage.NewStreamStorage(timeout),
	}
}

func (rh *requestHandler) createStream(c echo.Context) error {
	ID, err := rh.streamStorage.Create()
	if err != nil {
		log.Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	c.Response().Header().Add("Location", "/api/stream/"+ID)
	return c.NoContent(http.StatusCreated)
}

func (rh *requestHandler) getList(c echo.Context) error {
	streams, err := rh.streamStorage.List()
	if err != nil {
		log.Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.JSON(http.StatusOK, streams)
}

func (rh *requestHandler) getStream(c echo.Context) error {
	stream, err := rh.streamStorage.Get(c.Param("ID"))

	switch err {
	case StreamStorage.ErrStreamNotFound:
		return c.NoContent(http.StatusNotFound)
	case nil:
	default:
		log.Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.JSON(http.StatusOK, stream)
}

func (rh *requestHandler) runStream(c echo.Context) error {
	err := rh.streamStorage.Run(c.Param("ID"))
	switch err {
	case StreamStorage.ErrStreamNotFound:
		return c.NoContent(http.StatusNotFound)
	case StreamStorage.ErrStreamAlreadyActive, StreamStorage.ErrStreamFinished:
		// TODO: Returns error in json
		return c.NoContent(http.StatusBadRequest)
	case nil:
	default:
		log.Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	// TODO: No content?
	return c.NoContent(http.StatusNoContent)
}

func (rh *requestHandler) stopStream(c echo.Context) error {
	err := rh.streamStorage.Stop(c.Param("ID"))
	// TODO: Common error processing?
	switch err {
	case StreamStorage.ErrStreamNotFound:
		return c.NoContent(http.StatusNotFound)
	case StreamStorage.ErrStreamIsNotActive, StreamStorage.ErrStreamAlreadyInterrupted, StreamStorage.ErrStreamFinished:
		// TODO: Returns error in json
		return c.NoContent(http.StatusBadRequest)
	case nil:
	default:
		log.Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	// TODO: No content?
	return c.NoContent(http.StatusNoContent)
}

func main() {
	// TODO: timeout should be paramater
	handler := newRequestHandler(time.Second * 10)
	// Setup
	e := echo.New()
	e.Logger.SetLevel(log.INFO)

	// TODO: OPTIONS
	// TODO: groups?
	e.GET("/api/stream", handler.getList)
	e.GET("/api/stream/:ID", handler.getStream)
	e.POST("/api/stream", handler.createStream)
	e.PUT("/api/stream/:ID/run", handler.runStream)
	e.PUT("/api/stream/:ID/stop", handler.stopStream)

	// Start server
	go func() {
		if err := e.Start(":1323"); err != nil {
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
