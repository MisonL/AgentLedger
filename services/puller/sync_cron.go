package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

var syncCronExpressionParser = cron.NewParser(
	cron.SecondOptional |
		cron.Minute |
		cron.Hour |
		cron.Dom |
		cron.Month |
		cron.Dow |
		cron.Descriptor,
)

func (s *pullerService) syncCron(ctx context.Context, now time.Time) error {
	sources, err := s.listEnabledScheduledSources(ctx)
	if err != nil {
		return err
	}

	minuteStart, _ := cronMinuteWindow(now)
	scheduledSources, err := s.listScheduledSourceIDsForMinute(ctx, minuteStart)
	if err != nil {
		return err
	}

	for _, source := range sources {
		schedule, err := parseSyncCron(source.SyncCron)
		if err != nil {
			if s.log != nil {
				s.log.Warn("skip invalid sync_cron", "source_id", source.ID, "sync_cron", source.SyncCron, "error", err)
			}
			continue
		}

		_, alreadyScheduled := scheduledSources[source.ID]
		if !shouldEnqueueScheduledJob(schedule, now, alreadyScheduled) {
			continue
		}

		created, err := s.createScheduledSyncJob(ctx, source.ID, source.AccessMode, now)
		if err != nil {
			return err
		}
		if created {
			scheduledSources[source.ID] = struct{}{}
		}
	}

	return nil
}

func parseSyncCron(expression string) (cron.Schedule, error) {
	trimmed := strings.TrimSpace(expression)
	if trimmed == "" {
		return nil, fmt.Errorf("sync_cron is empty")
	}

	schedule, err := syncCronExpressionParser.Parse(trimmed)
	if err != nil {
		return nil, fmt.Errorf("parse sync_cron failed: %w", err)
	}
	return schedule, nil
}

func cronMinuteWindow(now time.Time) (time.Time, time.Time) {
	start := now.UTC().Truncate(time.Minute)
	return start, start.Add(time.Minute)
}

func cronScheduleHitsMinute(schedule cron.Schedule, now time.Time) bool {
	start, end := cronMinuteWindow(now)
	next := schedule.Next(start.Add(-time.Nanosecond))
	return !next.Before(start) && next.Before(end)
}

func shouldEnqueueScheduledJob(schedule cron.Schedule, now time.Time, alreadyScheduled bool) bool {
	if alreadyScheduled {
		return false
	}
	return cronScheduleHitsMinute(schedule, now)
}

func buildScheduledJobID(sourceID string, minuteStart time.Time) string {
	return stableID("syncjob", strings.TrimSpace(sourceID), "schedule", minuteStart.UTC().Format(time.RFC3339))
}

func normalizeSyncJobMode(raw string) string {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	switch normalized {
	case "realtime", "sync", "hybrid":
		return normalized
	default:
		return "realtime"
	}
}
