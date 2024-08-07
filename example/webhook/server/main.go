package main

import (
	"webhook/app"

	"github.com/gin-gonic/gin"

	"log"
	"net/http"
)

func main() {
	router := gin.Default()
	mutateServer := &(app.MutatingServer{})
	mutateServer.RegisterHandler(router)
	server := &http.Server{
		Addr:    ":443",
		Handler: router,
	}
	log.Printf("Starting webhook server at :%s...\n", server.Addr)
	log.Fatal(server.ListenAndServeTLS("/tls/tls.crt", "/tls/tls.key"))
}
