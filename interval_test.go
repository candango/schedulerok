package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewIntervalScheduleRejectsNonPositiveInterval(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Second} {
		_, err := NewIntervalSchedule(interval)
		assert.ErrorIs(t, err, ErrInvalidInterval)
	}
}

func TestIntervalScheduleNext(t *testing.T) {
	schedule, err := NewIntervalSchedule(5 * time.Minute)
	require.NoError(t, err)

	start := time.Date(2026, time.July, 10, 12, 0, 0, 0, time.UTC)
	assert.Equal(t, start.Add(5*time.Minute), schedule.Next(start))
}
