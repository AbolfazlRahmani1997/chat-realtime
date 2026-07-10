package router

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"server/internal/admin"
	"server/internal/ws"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

var r *gin.Engine

func InitRouter(wsHandler *ws.Handler, adminHandler *admin.Handler) {
	gin.SetMode(gin.ReleaseMode)
	gin.DisableConsoleColor()
	r = gin.Default()
	if f, err := os.Create("gin.log"); err == nil {
		gin.DefaultWriter = io.MultiWriter(f)
	}
	r.Use(cors.New(cors.Config{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{"GET", "POST"},
		AllowHeaders: []string{"*"},

		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		AllowOriginFunc: func(origin string) bool {
			return origin == "*"
		},
		MaxAge: 12 * time.Hour,
	}))

	//todo:create from rabbitmq
	r.POST("/chat/api/createRoom", wsHandler.CreateRoom)
	r.PUT("/chat/api/editRoom/:roomId", wsHandler.EditRoom)
	r.PUT("/chat/api/room/:roomId/avatar", wsHandler.SetRoomAvatar)
	r.GET("/chat/ws", wsHandler.UnifiedWS)
	//
	r.GET("/chat/ws/joinRoom/:roomId", wsHandler.JoinRoom)
	r.GET("/chat/ws/seenMessage/:roomId", wsHandler.ReadMessage)
	r.GET("/chat/ws/getRooms/", PGPToken(), wsHandler.GetRooms)
	r.GET("/chat/ws/syncRooms/", wsHandler.SyncRoom)
	r.GET("/chat/ws/getClients/:roomId", wsHandler.GetClients)
	r.POST("/chat/files", wsHandler.UploadFile)
	r.GET("/chat/files/:fileId", wsHandler.GetFile)
	r.GET("/chat/me", wsHandler.GetMe)
	r.GET("/chat/mock-users", wsHandler.GetMockUsers)
	r.GET("/chat/room/pin/:roomId", wsHandler.UpdatePin)
	r.GET("/chat/room/notification/:roomId", wsHandler.UpdateNotification)
	r.GET("/chat/user/last-seen/:userId", wsHandler.GetUserLastSeen)
	adminRouter := r.Group("/chat/api/admin")
	adminRouter.GET("/message/", adminHandler.GetAllMessages)
	adminRouter.GET("/message/:messageId", adminHandler.GetMessage)
	adminRouter.GET("/room/", adminHandler.FetchRooms)
	adminRouter.GET("/room/:roomId", adminHandler.FindRoom)
	adminRouter.PUT("/room/:roomId/deactive", adminHandler.DeactiveRoom)
	adminRouter.PUT("/room/:roomId/active", adminHandler.ActiveRoom)
}

func Start(addr string) error {
	return r.Run(addr)
}

func PGPToken() gin.HandlerFunc {

	return func(c *gin.Context) {
		//token := c.Query("token")
		//if token == "" {
		//	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Token is required"})
		//	return
		//}
		//
		//data := strings.Split(token, ".")
		//if len(data) != 3 {
		//	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Token is invalid"})
		//	return
		//}

		c.Next()
	}
}

func Auth(handler ws.Handler) gin.HandlerFunc {
	type User struct {
		Id        int    `json:"id"`
		Avatar    string `json:"avatar"`
		FirstName string `json:"firstname"`
		LastName  string `json:"lastname"`
		UserName  string `json:"username"`
	}

	return func(c *gin.Context) {
		token := c.Query("token")
		user, ok := handler.UserHandler[token]
		fmt.Print(ok)
		if ok {
			if user.LastStatusCode == 500 {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Token is required"})
			}
		}
		c.Next()

	}

}
