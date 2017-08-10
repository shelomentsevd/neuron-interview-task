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
	return c.JSON(http.StatusOK, "OK")
}

func (rh *requestHandler) getList(c echo.Context) error {
	return c.JSON(http.StatusOK, "OK")
}

func (rh *requestHandler) getStream(c echo.Context) error {
	return c.JSON(http.StatusOK, "OK")
}

func (rh *requestHandler) runStream(c echo.Context) error {
	return c.JSON(http.StatusOK, "OK")
}

func (rh *requestHandler) stopStream(c echo.Context) error {
	return c.JSON(http.StatusOK, "OK")
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
