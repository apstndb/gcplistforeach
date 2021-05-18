package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"os"
	"sync"
	"time"

	"github.com/itchyny/gojq"
	"github.com/lestrrat-go/backoff/v2"
	"go.uber.org/ratelimit"
	"golang.org/x/oauth2/google"
	"gopkg.in/yaml.v3"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// Caution: nextPageToken should be included in filter

func main() {
	if err := _main(); err != nil {
		panic(err)
	}
}

type params struct {
	BillingProject string
	collectionName string
	parallelism int64
	logHttp bool
	RawInput bool
	rateLimit int
	url string
	execute bool
	yamlInput bool
	yamlOutput bool
}

func parseParame() params {
	billingProject := flag.String("billing-project", "", "")
	collectionName := flag.String("collection", "", "collection field name for paginated list method")
	parallelism := flag.Int64("parallelism", 1, "")
	logHttp := flag.Bool("log-http", false, "")
	rawInput := flag.Bool("raw-input", false, "")
	rateLimit := flag.Int("rate-limit-per-minute", 0, "")
	url := flag.String("url", "", "")
	yamlInput := flag.Bool("yaml-input", false, "")
	yamlOutput := flag.Bool("yaml-output", false, "")
	execute := flag.Bool("execute", false, "")
	flag.Parse()
	return params{
		BillingProject: *billingProject,
		collectionName: *collectionName,
		parallelism:    *parallelism,
		logHttp:        *logHttp,
		RawInput:       *rawInput,
		url:            *url,
		execute:        *execute,
		rateLimit: *rateLimit,
		yamlInput: *yamlInput,
		yamlOutput: *yamlOutput,
	}
}

type lineDecoder struct{
	input *bufio.Scanner
}

type output struct {
	Input interface{} `json:"input"`
	Response interface{} `json:"response"`
}

func(d *lineDecoder) Decode(i interface{}) error {
	if !d.input.Scan() {
		return d.input.Err()
	}
	*(i.(*interface{})) = d.input.Text()
	return nil
}

func _main() error {
	params := parseParame()

	var rl ratelimit.Limiter
	if params.rateLimit != 0 {
		rl = ratelimit.New(params.rateLimit, ratelimit.Per(time.Minute))
	} else {
		rl = ratelimit.NewUnlimited()
	}

	backoffPolicy := backoff.Exponential(
		backoff.WithMinInterval(1 * time.Second),
		backoff.WithMaxInterval(time.Minute),
		backoff.WithJitterFactor(0.1))

	query, err := gojq.Parse(fmt.Sprintf(`%s`, params.url))
	if err != nil {
		return err
	}
	code, err := gojq.Compile(query)
	if err != nil {
		return err
	}

	ctx := context.Background()
	client, err := google.DefaultClient(ctx)
	if err != nil {
		return err
	}

	var enc interface {
		Encode(interface{}) error
	}
	if params.yamlOutput {
		enc = yaml.NewEncoder(os.Stdout)
	} else {
		enc = json.NewEncoder(os.Stdout)
	}

	sem := semaphore.NewWeighted(params.parallelism)
	var muStderr sync.Mutex
	var muStdout sync.Mutex

	var dec interface {
		Decode(interface{}) error
	}

	if params.RawInput {
		dec = &lineDecoder{bufio.NewScanner(os.Stdin)}
	} else if params.yamlInput {
		dec = yaml.NewDecoder(os.Stdin)
	} else {
		dec = json.NewDecoder(os.Stdin)
	}

	eg, ctx := errgroup.WithContext(ctx)
	for {
		var input interface{}
		if err := dec.Decode(&input); err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		run := code.Run(input)
		for {
			i, ok := run.Next()
			if !ok {
				break
			}
			url, ok := i.(string)
			if !ok {
				return fmt.Errorf("not string: %v", i)
			}
			eg.Go(func() error {
				if err := sem.Acquire(ctx, 1); err != nil {
					return err
				}
				defer sem.Release(1)
				var nextPageToken string
				var collection []interface{}
				var result output
				for {
					req, err := http.NewRequest(http.MethodGet, url, nil)
					if err != nil {
						return err
					}
					q := req.URL.Query()
					if nextPageToken != "" {
						q.Add("pageToken", nextPageToken)
					}
					req.URL.RawQuery = q.Encode()
					if params.BillingProject != "" {
						req.Header.Add("x-goog-user-project", params.BillingProject)
					}
					if !params.execute {
						muStderr.Lock()
						fmt.Fprintf(os.Stderr, "%s %s\n", req.Method, req.URL.String())
						muStderr.Unlock()
						return nil
					}

					backoffCtl := backoffPolicy.Start(ctx)
					resp, err := func() (*http.Response, error) {
						for backoff.Continue(backoffCtl) {
							resp, err := func() (*http.Response, error) {
								var buf bytes.Buffer
								if params.logHttp {
									defer func() {
										muStderr.Lock()
										defer muStderr.Unlock()
										io.Copy(os.Stderr, &buf)
									}()
								}
								b, _ := httputil.DumpRequest(req, true)
								buf.Write(b)

								rl.Take()
								resp, err := client.Do(req)
								if err != nil {
									return nil, err
								}
								b, _ = httputil.DumpResponse(resp, true)
								buf.Write(b)
								return resp, nil
							}()

							if err != nil {
								return resp, err
							} else if resp.StatusCode == http.StatusOK {
								return resp, nil
							} else if resp.StatusCode == http.StatusTooManyRequests  {
								continue
							} else if resp.StatusCode >= 400 && resp.StatusCode < 500{
								return resp, nil
							}
						}
						return nil, errors.New("backoff finally failed")
					}()

					body, err := io.ReadAll(resp.Body)
					if err != nil {
						return err
					}

					var i map[string]interface{}
					err = json.Unmarshal(body, &i)
					if err != nil {
						return err
					}

					if params.collectionName == "" || resp.StatusCode != http.StatusOK {
						result = output{
							Input: input,
							Response:  i,
						}
						break
					}

					if c, ok := i[params.collectionName].([]interface{}); ok {
						collection = append(collection, c...)
					}
					if npt, ok := i["nextPageToken"].(string); ok {
						nextPageToken = npt
						continue
					}
					response := make(map[string]interface{})
					// leave response empty if collection is nil
					if collection != nil {
						response[params.collectionName] = collection
					}
					result = output{
						Input: input,
						Response: response,
					}
					break
				}

				return func() error {
					muStdout.Lock()
					defer muStdout.Unlock()
					return enc.Encode(result)
				}()
			})
		}
	}
	return eg.Wait()
}
