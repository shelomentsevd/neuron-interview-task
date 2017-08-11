package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
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

func (rh *requestHandler) finishStream(c echo.Context) error {
	err := rh.streamStorage.Finish(c.Param("ID"))

	switch err {
	case StreamStorage.ErrStreamNotFound:
		return c.NoContent(http.StatusNotFound)
	case StreamStorage.ErrStreamIsNotInterrupted:
		// TODO: Returns error in json
		return c.NoContent(http.StatusBadRequest)
	case nil:
	default:
		log.Error(err)
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.NoContent(http.StatusNoContent)
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
	e.PUT("/api/stream/:ID/run", handler.runStream)
	e.PUT("/api/stream/:ID/stop", handler.stopStream)
	e.PUT("/api/stream/:ID/finish", handler.finishStream)

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
