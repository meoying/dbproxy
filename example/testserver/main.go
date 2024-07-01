package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/spf13/viper"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
)

// Order 结构体定义
type Order struct {
	OrderId int     `json:"orderId"`
	UserId  int64   `json:"userId"`
	Content string  `json:"content"`
	Account float64 `json:"account"`
}

// 数据库连接全局变量
var db *sql.DB

// InitDB 从配置文件初始化数据库连接
func InitDB(configFile string) error {
	// 加载配置文件
	viper.SetConfigFile(configFile)
	if err := viper.ReadInConfig(); err != nil {
		return err
	}

	// 从配置文件中获取数据库连接信息
	dsn := viper.GetString("db.dsn")

	// 连接 MySQL 数据库
	var err error
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return err
	}

	// 测试数据库连接
	if err := db.PingContext(context.Background()); err != nil {
		return err
	}

	return nil
}

func main() {
	var err error
	// 连接 MySQL 数据库
	err = InitDB("etc/config.yaml")
	if err != nil {
		panic(err)
	}

	// 初始化 Gin
	r := gin.Default()

	// 设置路由
	r.POST("/order", createOrder)
	r.GET("/order/:id", getOrder)
	r.PUT("/order/:id", updateOrder)
	r.DELETE("/order/:id", deleteOrder)

	// 启动服务器
	r.Run(":8080")
}

// 创建订单处理函数
func createOrder(c *gin.Context) {
	var order Order
	if err := c.BindJSON(&order); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 指定 order_id 字段的值
	insertSQL := fmt.Sprintf("INSERT INTO orders (order_id, user_id, content, account) VALUES (%d, %d, '%s', %f)",
		order.OrderId, order.UserId, order.Content, order.Account)

	// 执行插入操作
	_, err := db.Exec(insertSQL)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Order created successfully"})
}

// 获取订单处理函数
func getOrder(c *gin.Context) {
	orderId, _ := strconv.ParseInt(c.Param("id"), 10, 64)

	querySQL := "SELECT order_id, user_id, content, account FROM orders WHERE order_id = ?"
	var order Order
	err := db.QueryRow(querySQL, orderId).Scan(&order.OrderId, &order.UserId, &order.Content, &order.Account)
	switch {
	case err == sql.ErrNoRows:
		c.JSON(http.StatusNotFound, gin.H{"error": "Order not found"})
		return
	case err != nil:
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, order)
}

// 更新订单处理函数
func updateOrder(c *gin.Context) {
	orderId, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	var updatedOrder Order
	if err := c.BindJSON(&updatedOrder); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	updateSQL := fmt.Sprintf("UPDATE orders SET user_id = %d, content = '%s', account = %f WHERE order_id = %d",
		updatedOrder.UserId, updatedOrder.Content, updatedOrder.Account, orderId)

	_, err := db.Exec(updateSQL)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Order updated successfully"})
}

// 删除订单处理函数
func deleteOrder(c *gin.Context) {
	orderId, _ := strconv.ParseInt(c.Param("id"), 10, 64)

	deleteSQL := fmt.Sprintf("DELETE FROM orders WHERE order_id = %d", orderId)

	_, err := db.Exec(deleteSQL)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Order deleted successfully"})
}
