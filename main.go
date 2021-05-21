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
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/itchyny/gojq"
	_ "github.com/jessevdk/go-flags"
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
	BillingProject string `long:"billing-project"`
	CollectionName string `long:"collection"`
	Parallelism    int64  `long:"parallelism" default:"1"`
	LogHttp        bool   `long:"log-http"`
	RawInput       bool   `long:"raw-input"`
	RateLimit      int    `long:"rate-limit-per-minute"`
	Url            string `long:"url"`
	Execute        bool   `long:"execute"`
	Verbose        bool   `long:"verbose"`
	AutoCollection bool   `long:"auto-collection"`
	YamlInput      bool   `long:"yaml-input"`
	FilterError    bool   `long:"filter-error"`
	YamlOutput     bool   `long:"yaml-output"`
}

func parseParame() (params, error) {
	// flagParser := flags.NewParser(&o, flags.Default)
	// _, err := flagParser.ParseArgs()
	// if err != nil {
	// 	return o, err
	// }
	// return o, nil
	billingProject := flag.String("billing-project", "", "")
	collectionName := flag.String("collection", "", "collection field name for paginated list method")
	parallelism := flag.Int64("parallelism", 1, "")
	filterError := flag.Bool("filter-error", false, "")
	logHttp := flag.Bool("log-http", false, "")
	autoCollection := flag.Bool("auto-collection", false, "Infer collection name from URL path")
	rawInput := flag.Bool("raw-input", false, "")
	rateLimit := flag.Int("rate-limit-per-minute", 0, "")
	urlGenerator := flag.String("url", "", "")
	verbose := flag.Bool("verbose", false, "")
	yamlInput := flag.Bool("yaml-input", false, "")
	yamlOutput := flag.Bool("yaml-output", false, "")
	execute := flag.Bool("execute", false, "")
	flag.Parse()
	return params{
		BillingProject: *billingProject,
		CollectionName: *collectionName,
		Parallelism:    *parallelism,
		LogHttp:        *logHttp,
		FilterError:    *filterError,
		Verbose:        *verbose,
		RawInput:       *rawInput,
		Url:            *urlGenerator,
		AutoCollection: *autoCollection,
		Execute:        *execute,
		RateLimit:      *rateLimit,
		YamlInput:      *yamlInput,
		YamlOutput:     *yamlOutput,
	}, nil
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
		if d.input.Err() == nil {
			return io.EOF
		}
		return d.input.Err()
	}
	*(i.(*interface{})) = d.input.Text()
	return nil
}

func _main() error {
	params, err := parseParame()
	if err != nil {
		return err
	}

	if params.AutoCollection && params.CollectionName != "" {
		return errors.New("--auto-collection and --collection are exclusive")
	}

	var rl ratelimit.Limiter
	if params.RateLimit != 0 {
		rl = ratelimit.New(params.RateLimit, ratelimit.Per(time.Minute))
	} else {
		rl = ratelimit.NewUnlimited()
	}

	backoffPolicy := backoff.Exponential(
		backoff.WithMinInterval(1 * time.Second),
		backoff.WithMaxInterval(time.Minute),
		backoff.WithJitterFactor(0.1))

	query, err := gojq.Parse(fmt.Sprintf(`%s`, params.Url))
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
	if params.YamlOutput {
		enc = yaml.NewEncoder(os.Stdout)
	} else {
		enc = json.NewEncoder(os.Stdout)
	}

	sem := semaphore.NewWeighted(params.Parallelism)
	var muStderr sync.Mutex
	var muStdout sync.Mutex

	var dec interface {
		Decode(interface{}) error
	}

	if params.RawInput {
		dec = &lineDecoder{bufio.NewScanner(os.Stdin)}
	} else if params.YamlInput {
		dec = yaml.NewDecoder(os.Stdin)
	} else {
		dec = json.NewDecoder(os.Stdin)
	}

	var count int
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
			baseUrl, ok := i.(string)
			if !ok {
				return fmt.Errorf("not string: %v", i)
			}

			nowCount := count
			count++

			var collectionName string
			if params.CollectionName != "" {
				collectionName = params.CollectionName
			} else if params.AutoCollection {
				u, err := url.Parse(baseUrl)
				if err != nil {
					return err
				}

				pathElems := strings.Split(u.Path, "/")
				collectionName = pathElems[len(pathElems)-1]
			}

			// Acquire semaphore before eg.Go to stabilize output order when parallelism=1
			if err := sem.Acquire(ctx, 1); err != nil {
				return err
			}
			eg.Go(func() error {
				defer sem.Release(1)
				var nextPageToken string
				var collection []interface{}
				var result output
				for {
					req, err := http.NewRequest(http.MethodGet, baseUrl, nil)
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
					if !params.Execute || params.Verbose {
						muStderr.Lock()
						log.Printf("do url[%v]: %v %v\n", nowCount, req.Method, req.URL.String())
						muStderr.Unlock()
					}
					if !params.Execute {
						return nil
					}

					backoffCtl := backoffPolicy.Start(ctx)
					resp, err := func() (*http.Response, error) {
						for backoff.Continue(backoffCtl) {
							resp, err := func() (*http.Response, error) {
								var buf bytes.Buffer
								if params.LogHttp {
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
								log.Printf("retry url[%v]: %v %v, reason: %v\n", nowCount, resp.Request.Method, resp.Request.URL.String(), resp.Status)
								continue
							} else if resp.StatusCode >= 400 && resp.StatusCode < 500{
								log.Printf("error url[%v]: %v %v, reason: %v\n", nowCount,resp.Request.Method, resp.Request.URL.String(), resp.Status)
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

					if params.FilterError && resp.StatusCode != http.StatusOK {
						return nil
					}

					if collectionName == "" || resp.StatusCode != http.StatusOK {
						result = output{
							Input: input,
							Response:  i,
						}
						break
					}

					if c, ok := i[collectionName].([]interface{}); ok {
						collection = append(collection, c...)
					}
					if npt, ok := i["nextPageToken"].(string); ok {
						nextPageToken = npt
						continue
					}
					response := make(map[string]interface{})
					// leave response empty if collection is nil
					if collection != nil {
						response[collectionName] = collection
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
