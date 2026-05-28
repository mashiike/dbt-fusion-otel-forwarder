package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestRetryExporter_UploadTraces_SucceedsAfterRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockExporter(ctrl)
	gomock.InOrder(
		mock.EXPECT().UploadTraces(gomock.Any(), gomock.Any()).Return(errors.New("transient")),
		mock.EXPECT().UploadTraces(gomock.Any(), gomock.Any()).Return(errors.New("transient")),
		mock.EXPECT().UploadTraces(gomock.Any(), gomock.Any()).Return(nil),
	)

	exp := &RetryExporter{
		Exporter:      mock,
		MaxAttempts:   3,
		RetryInterval: 1 * time.Millisecond,
	}
	err := exp.UploadTraces(context.Background(), nil)
	require.NoError(t, err)
}

func TestRetryExporter_UploadLogs_GivesUpAfterMaxAttempts(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	wantErr := errors.New("persistent failure")
	mock := NewMockExporter(ctrl)
	mock.EXPECT().UploadLogs(gomock.Any(), gomock.Any()).Return(wantErr).Times(3)

	exp := &RetryExporter{
		Exporter:      mock,
		MaxAttempts:   3,
		RetryInterval: 1 * time.Millisecond,
	}
	err := exp.UploadLogs(context.Background(), nil)
	require.Error(t, err)
	assert.Equal(t, wantErr, err)
}

func TestRetryExporter_AbortsOnContextCancel(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockExporter(ctrl)
	mock.EXPECT().UploadTraces(gomock.Any(), gomock.Any()).Return(errors.New("fail")).Times(1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	exp := &RetryExporter{
		Exporter:      mock,
		MaxAttempts:   5,
		RetryInterval: 1 * time.Second,
	}
	err := exp.UploadTraces(ctx, nil)
	require.Error(t, err)
}

func TestRetryExporter_NoRetryWhenMaxAttemptsIsOne(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockExporter(ctrl)
	mock.EXPECT().UploadLogs(gomock.Any(), gomock.Any()).Return(errors.New("fail")).Times(1)

	exp := &RetryExporter{
		Exporter:      mock,
		MaxAttempts:   1,
		RetryInterval: 1 * time.Millisecond,
	}
	err := exp.UploadLogs(context.Background(), nil)
	require.Error(t, err)
}
