/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, sub to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package log

import (
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/jeffail/gabs"
)

/*--------------------------------------------------------------------------------------------------
 */

/*
StatsConfig - Holds configuration options for a stats object.
*/
type StatsConfig struct {
	JobBuffer      int64  `json:"job_buffer" yaml:"job_buffer"`
	RootPath       string `json:"prefix" yaml:"prefix"`
	RetainInternal bool   `json:"retain_internal" yaml:"retain_internal"`
	PushInterval   int64  `json:"push_interval_ms" yaml:"push_interval_ms"`
}

/*
DefaultStatsConfig - Returns a fully defined stats configuration with the default values for each
field.
*/
func DefaultStatsConfig() StatsConfig {
	return StatsConfig{
		JobBuffer:      100,
		RootPath:       "service",
		RetainInternal: true,
		PushInterval:   1000,
	}
}

/*--------------------------------------------------------------------------------------------------
 */

// Errors for the Stats type.
var (
	ErrStatsNotTracked = errors.New("internal stats not being tracked")
	ErrTimedOut        = errors.New("timed out")
)

/*
Stats - A stats object with capability to hold internal stats as a JSON endpoint.
*/
type Stats struct {
	config        StatsConfig
	jsonRoot      *gabs.Container
	json          *gabs.Container
	flatStats     map[string]interface{}
	pathPrefix    string
	timestamp     time.Time
	jobChan       chan func()
	riemannClient *RiemannClient
}

/*
NewStats - Create and return a new stats object.
*/
func NewStats(config StatsConfig) *Stats {
	var jsonRoot, json *gabs.Container
	var pathPrefix string

	if config.RetainInternal {
		jsonRoot = gabs.New()
		if len(config.RootPath) > 0 {
			pathPrefix = config.RootPath + "."
			json, _ = jsonRoot.SetP(map[string]interface{}{}, config.RootPath)
		} else {
			json = jsonRoot
		}
	}
	stats := Stats{
		config:     config,
		jsonRoot:   jsonRoot,
		json:       json,
		flatStats:  map[string]interface{}{},
		pathPrefix: pathPrefix,
		timestamp:  time.Now(),
		jobChan:    make(chan func(), config.JobBuffer),
	}
	go stats.loop()
	return &stats
}

/*
UseRiemann - Register a RiemannClient object to be used for pushing stats to a riemann service.
*/
func (s *Stats) UseRiemann(client *RiemannClient) error {
	if client == nil {
		return ErrClientNil
	}
	s.riemannClient = client
	return nil
}

/*
Close - Stops the stats object from accepting stats.
*/
func (s *Stats) Close() {
	jChan := s.jobChan
	s.jobChan = nil
	close(jChan)

	// Closure is done elsewhere since this client might be shared.
	s.riemannClient = nil
}

/*--------------------------------------------------------------------------------------------------
 */

/*
GetStats - Returns a string containing the JSON serialized structure of stats at the time of the
request.
*/
func (s *Stats) GetStats(timeout time.Duration) (string, error) {
	responseChan := make(chan string, 1)
	errorChan := make(chan error, 1)

	s.Incr("stats.requests", 1)
	s.jobChan <- func() {
		if nil != s.json {
			s.updateInternals()
			select {
			case responseChan <- s.jsonRoot.String():
			default:
			}
		} else {
			errorChan <- ErrStatsNotTracked
		}
	}

	select {
	case stats := <-responseChan:
		return stats, nil
	case err := <-errorChan:
		return "", err
	case <-time.After(timeout):
	}
	return "", ErrTimedOut
}

/*--------------------------------------------------------------------------------------------------
 */

/*
Incr - Increment a stat by a value.
*/
func (s *Stats) Incr(stat string, value int) {
	s.jobChan <- func() {
		total, _ := s.flatStats[stat].(int)
		total += value
		s.flatStats[stat] = total

		if nil != s.json {
			s.json.SetP(total, stat)
		}
	}
}

/*
Decr - Decrement a stat by a value.
*/
func (s *Stats) Decr(stat string, value int) {
	s.jobChan <- func() {
		total, _ := s.flatStats[stat].(int)
		total -= value
		s.flatStats[stat] = total

		if nil != s.json {
			s.json.SetP(total, stat)
		}
	}
}

/*
Timing - Set a stat representing a duration.
*/
func (s *Stats) Timing(stat string, delta float64) {
	s.jobChan <- func() {
		s.flatStats[stat] = delta
		if nil != s.json {
			s.json.SetP(fmt.Sprintf("%vs", delta), stat)
		}
		if nil != s.riemannClient {
			s.riemannClient.SendEvent(RiemannEvent{
				Service: s.pathPrefix + stat,
				Metric:  delta,
				Tags:    []string{"stat"},
				TTL:     float32(s.config.PushInterval*2) / 1000,
			})
		}
	}
}

/*
Gauge - Set a stat as a gauge value.
*/
func (s *Stats) Gauge(stat string, value float64) {
	s.jobChan <- func() {
		s.flatStats[stat] = value
		if nil != s.json {
			s.json.SetP(value, stat)
		}
		if nil != s.riemannClient {
			s.riemannClient.SendEvent(RiemannEvent{
				Service: s.pathPrefix + stat,
				Metric:  value,
				Tags:    []string{"stat"},
				TTL:     float32(s.config.PushInterval*2) / 1000,
			})
		}
	}
}

/*--------------------------------------------------------------------------------------------------
 */

/*
updateInternals - Update stats such as uptime and num goroutines.
*/
func (s *Stats) updateInternals() {
	uptime := time.Since(s.timestamp).Seconds()
	goroutines := runtime.NumGoroutine()

	s.flatStats["uptime"] = uptime
	s.flatStats["goroutines"] = goroutines
	if nil != s.json {
		s.json.SetP(fmt.Sprintf("%vs", uptime), "uptime")
		s.json.SetP(goroutines, "goroutines")
	}
}

/*
loop - Internal loop of the logger, simply consumes a queue of funcs, and runs them within a single
goroutine.
*/
func (s *Stats) loop() {
	pushPeriod := time.Duration(s.config.PushInterval) * time.Millisecond
	pushTimer := time.NewTimer(pushPeriod)

	var job func()
	var running = true

	for running {
		select {
		case job, running = <-s.jobChan:
			if running {
				job()
			}
		case <-pushTimer.C:
			s.updateInternals()
			if s.riemannClient != nil {
				events := []RiemannEvent{}
				for flatStat, value := range s.flatStats {
					events = append(events, RiemannEvent{
						Service: s.pathPrefix + flatStat,
						Metric:  value,
						Tags:    []string{"stat"},
						TTL:     float32(s.config.PushInterval*2) / 1000,
					})
				}
				s.riemannClient.SendEvents(events)
			}
			pushTimer.Reset(pushPeriod)
		}
	}
}

/*--------------------------------------------------------------------------------------------------
 */
