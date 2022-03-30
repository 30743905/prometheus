package chunkenc

import (
	"fmt"
	"testing"
	"time"
)

func TestXor(t *testing.T) {

	c := NewXORChunk()
	app, err := c.Appender()
	if err != nil {
		fmt.Println("error")
	}

	for i := 0; i < 10; i++ {
		t1 := time.Now().Unix()
		v1 := 10.1 + float64(i)
		app.Append(t1, v1)
	}
	fmt.Println("========>")

}
