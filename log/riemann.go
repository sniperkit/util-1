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

	"github.com/bigdatadev/goryman"
)

/*--------------------------------------------------------------------------------------------------
 */

/*
RiemannClientConfig - Configuration options for connecting to a riemann service.
*/
type RiemannClientConfig struct {
	Address   string `json:"address" yaml:"address"`
	JobBuffer int64  `json:"job_buffer" yaml:"job_buffer"`
}

/*
NewRiemannClientConfig - Returns a RiemannClientConfig populated with default values.
*/
func NewRiemannClientConfig() RiemannClientConfig {
	return RiemannClientConfig{
		Address:   "",
		JobBuffer: 100,
	}
}

/*--------------------------------------------------------------------------------------------------
 */

// Errors for the RiemannClient type.
var (
	ErrEmptyConfigAddress = errors.New("address config value is empty")
	ErrInvalidConfig      = errors.New("invalid config value")
)

/*
RiemannClient - Connect to a riemann service, this struct simply wraps a third party library which
actually implements the client protocol.
*/
type RiemannClient struct {
	config  RiemannClientConfig
	rClient *goryman.GorymanClient
	jobChan chan func()
}

/*
NewRiemannClient - Create a new RiemannClient based on a configuration object, returns an error
*/
func NewRiemannClient(config RiemannClientConfig) (*RiemannClient, error) {
	if 0 == len(config.Address) {
		return nil, ErrEmptyConfigAddress
	}

	c := goryman.NewGorymanClient(config.Address)
	err := c.Connect()
	if err != nil {
		return nil, err
	}

	client := RiemannClient{
		config:  config,
		rClient: c,
		jobChan: make(chan func(), config.JobBuffer),
	}
	go client.loop()

	return &client, nil
}

func (r *RiemannClient) loop() {
	for job := range r.jobChan {
		job()
	}
}

/*
Close - Close the connection to the Riemann service.
*/
func (r *RiemannClient) Close() {
	jChan := r.jobChan
	r.jobChan = nil
	close(jChan)

	r.rClient.Close()
}

/*--------------------------------------------------------------------------------------------------
 */

// RiemannEvent - A library agnostic representation of a Riemann event.
type RiemannEvent struct {
	Service     string
	State       string
	Description string
	Metric      interface{}
	Tags        []string
}

// SendEvent - Send an event, this call is non-blocking and does not guarantee receipt.
func (r *RiemannClient) SendEvent(event RiemannEvent) {
	r.jobChan <- func() {
		r.rClient.SendEvent(&goryman.Event{
			Service:     event.Service,
			State:       event.State,
			Description: event.Description,
			Metric:      event.Metric,
			Tags:        event.Tags,
		})
	}
}

/*--------------------------------------------------------------------------------------------------
 */
