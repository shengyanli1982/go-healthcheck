package tcp

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/shengyanli1982/go-healthcheck/checker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTCPChecker_Connect(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	c, err := checker.NewChecker("tcp", checker.TargetConfig{Addr: ln.Addr().String()})
	require.NoError(t, err)
	require.NotNil(t, c)

	result, err := c.Check(context.Background())
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.True(t, result.Healthy)
	assert.Equal(t, 0, result.StatusCode, "TCP has no status code, always 0")
	assert.Greater(t, result.Latency, time.Duration(0))
	assert.NoError(t, result.Err)
	assert.True(t, result.Timestamp.IsZero(), "builtin Checker leaves Timestamp zero (M-6)")
}

func TestTCPChecker_Refused(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	c, err := checker.NewChecker("tcp", checker.TargetConfig{Addr: addr})
	require.NoError(t, err)

	result, err := c.Check(context.Background())
	require.NoError(t, err, "connection refused takes the factual-return path (result, nil) (M-5)")
	require.NotNil(t, result)
	assert.False(t, result.Healthy)
	assert.Error(t, result.Err)
	assert.Equal(t, 0, result.StatusCode)
}

func TestTCPChecker_CancelledContext(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	c, err := checker.NewChecker("tcp", checker.TargetConfig{Addr: ln.Addr().String()})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	var result *checker.CheckResult
	var checkErr error
	go func() {
		defer close(done)
		result, checkErr = c.Check(ctx)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Check blocked without returning under cancelled ctx (select timeout prevents deadlock)")
	}

	// result 或 error 形态均可（任务卡口径），但必须非健康。
	if checkErr != nil {
		return
	}
	require.NotNil(t, result)
	assert.False(t, result.Healthy)
	assert.Error(t, result.Err)
}

func TestTCPChecker_FactoryErrors(t *testing.T) {
	c, err := checker.NewChecker("tcp", checker.TargetConfig{Addr: ""})
	require.Error(t, err)
	assert.Nil(t, c)
	assert.Contains(t, err.Error(), "empty target address")
	assert.ErrorIs(t, err, checker.ErrEmptyTargetAddress) // 三个子包共用的 sentinel（docs/06 §5）
}
