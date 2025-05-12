package router

import "github.com/gin-gonic/gin"

// NewRouter returns a new router
func NewRouter() *gin.Engine {
	return gin.Default()
}
