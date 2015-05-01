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

	s.jobChan <- func() {
		if nil != s.json {
			s.Incr("stats.requests", 1)
			s.Timing("uptime", (time.Since(s.timestamp).Nanoseconds() / 1e6))
			s.Gauge("goroutines", int64(runtime.NumGoroutine()))
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
func (s *Stats) Incr(stat string, value int64) {
	s.jobChan <- func() {
		if nil != s.json {
			if target, ok := s.json.Path(stat).Data().(int64); ok {
				s.json.SetP(target+value, stat)
			} else {
				s.json.SetP(value, stat)
			}
		}
		if nil != s.riemannClient {
			s.riemannClient.SendEvent(RiemannEvent{
				Service: s.pathPrefix + stat,
				Metric:  value,
			})
		}
	}
}

/*
Decr - Decrement a stat by a value.
*/
func (s *Stats) Decr(stat string, value int64) {
	s.jobChan <- func() {
		if nil != s.json {
			if target, ok := s.json.Path(stat).Data().(int64); ok {
				s.json.SetP(target-value, stat)
			} else {
				s.json.SetP(0-value, stat)
			}
		}
		if nil != s.riemannClient {
			s.riemannClient.SendEvent(RiemannEvent{
				Service: s.pathPrefix + stat,
				Metric:  0 - value,
			})
		}
	}
}

/*
Timing - Set a stat representing a duration.
*/
func (s *Stats) Timing(stat string, delta int64) {
	s.jobChan <- func() {
		if nil != s.json {
			s.json.SetP(fmt.Sprintf("%vms", delta), stat)
		}
		if nil != s.riemannClient {
			s.riemannClient.SendEvent(RiemannEvent{
				Service: s.pathPrefix + stat,
				Metric:  delta,
			})
		}
	}
}

/*
Gauge - Set a stat as a gauge value.
*/
func (s *Stats) Gauge(stat string, value int64) {
	s.jobChan <- func() {
		if nil != s.json {
			s.json.SetP(value, stat)
		}
		if nil != s.riemannClient {
			s.riemannClient.SendEvent(RiemannEvent{
				Service: s.pathPrefix + stat,
				Metric:  value,
			})
		}
	}
}

/*--------------------------------------------------------------------------------------------------
 */

/*
loop - Internal loop of the logger, simply consumes a queue of funcs, and runs them within a single
goroutine.
*/
func (s *Stats) loop() {
	for job := range s.jobChan {
		job()
	}
}

/*--------------------------------------------------------------------------------------------------
 */
