package frontend

import (
	"fmt"
	"strings"
	"time"
)

func shortenID(id string) string {
	n := len(id)
	if n <= 10 {
		return id
	}
	return id[:5] + "..." + id[n-5:]
}

func humanizeArbTime(t time.Time) string {
	now := time.Now()

	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	eventDay := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())

	daysAgo := int(today.Sub(eventDay).Hours() / 24)

	switch {
	case daysAgo == 0:
		return fmt.Sprintf("today: %s", t.Format("03:04 PM"))

	case daysAgo == 1:
		return fmt.Sprintf("yesterday: %s", t.Format("03:04 PM"))

	case daysAgo > 1 && daysAgo <= 6:
		return fmt.Sprintf("%dd ago (%s): %s",
			daysAgo,
			t.Format("Mon"),
			t.Format("03:04 PM"),
		)

	default:
		return t.Format("Mon 02 Jan 2006 03:04 PM")
	}
}

func friendlyTime(t time.Time) string {
	now := time.Now().In(t.Location()) // Ensure we compare in the same timezone

	days := daysBetween(t, now)

	timeStr := t.Format("03:04 PM")

	switch {
	case days == 0:
		return fmt.Sprintf("Today: %s", timeStr)
	case days == 1:
		return fmt.Sprintf("Yesterday: %s", timeStr)
	case days > 1 && days <= 6:
		dayName := t.Format("Mon")
		return fmt.Sprintf("%dd ago (%s): %s", days, dayName, timeStr)
	default:
		return t.Format("Mon 02 Jan 2006 03:04 PM")
	}
}

func daysBetween(a, b time.Time) int {
	aY, aM, aD := a.Date()
	bY, bM, bD := b.Date()

	aMidnight := time.Date(aY, aM, aD, 0, 0, 0, 0, a.Location())
	bMidnight := time.Date(bY, bM, bD, 0, 0, 0, 0, b.Location())

	return int(bMidnight.Sub(aMidnight).Hours() / 24)
}

func ParseOutcomes(raw string) []string {
	return strings.Split(raw, ",")
}

func GetOutcomeByIndex(outcomes []string, idx int) string {
	if idx >= 0 && idx < len(outcomes) {
		s := strings.TrimSpace(outcomes[idx])
		r := strings.NewReplacer("[", "", "]", "")

		return r.Replace(s)
	}
	return "?"
}
