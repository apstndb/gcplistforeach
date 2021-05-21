package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/itchyny/gojq"
	"github.com/jessevdk/go-flags"
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

type opts struct {
	BillingProject string `long:"billing-project" env:"GCLOUD_BILLING_QUOTA_PROJECT"`
	Parallelism    int64  `long:"parallelism" default:"1"`
	LogHttp        bool   `long:"log-http"`
	RateLimit      int    `long:"rate-limit-per-minute"`
	Url            string `long:"url" description:"URL generator written by jq filter" unquote:"false"`
	Execute        bool   `long:"execute" description:"Execute without dry-run"`
	Verbose        bool   `long:"verbose"`
	CollectionName string `long:"collection" description:"Collection name in favor of AIP-132 for paging (exclusive with --auto-collection"`
	AutoCollection bool   `long:"auto-collection" description:"Infer collection name from URL for paging (exclusive with --collection)"`
	YamlInput      bool   `long:"yaml-input"`
	RawInput       bool   `long:"raw-input"`
	IncludeError   bool   `long:"include-error"`
	YamlOutput     bool   `long:"yaml-output"`
}

func isErrHelp(err error) bool {
	if e, ok := err.(*flags.Error); ok && e.Type == flags.ErrHelp {
		return true
	}
	return false
}
func parseOpts() (o opts, err error) {
	flagParser := flags.NewParser(&o, flags.Default)
	defer func() {
		if err == nil {
			return
		}
		if isErrHelp(err) {
			return
		}
		log.Print(err)
		flagParser.WriteHelp(os.Stderr)
	}()
	_, err = flagParser.Parse()
	if err != nil {
		return o, err
	}
	if o.YamlInput && o.RawInput {
		return o, errors.New("--yaml-input and --raw-input are exclusive")
	}
	if o.AutoCollection && o.CollectionName != "" {
		return o, errors.New("--auto-collection and --collection are exclusive")
	}
	return o, nil
}

// adapter for bufio.Scanner
type lineDecoder struct {
	input *bufio.Scanner
}

type output struct {
	Input    interface{} `json:"input"`
	Response interface{} `json:"response"`
}

func (d *lineDecoder) Decode(i interface{}) error {
	if !d.input.Scan() {
		if d.input.Err() == nil {
			return io.EOF
		}
		return d.input.Err()
	}
	reflect.Indirect(reflect.ValueOf(i)).Set(reflect.ValueOf(d.input.Text()))
	return nil
}

func _main() error {
	opts, err := parseOpts()
	if err != nil {
		os.Exit(1)
	}

	if opts.AutoCollection && opts.CollectionName != "" {
		return errors.New("--auto-collection and --collection are exclusive")
	}

	var rl ratelimit.Limiter
	if opts.RateLimit != 0 {
		rl = ratelimit.New(opts.RateLimit, ratelimit.Per(time.Minute))
	} else {
		rl = ratelimit.NewUnlimited()
	}

	backoffPolicy := backoff.Exponential(
		backoff.WithMinInterval(1*time.Second),
		backoff.WithMaxInterval(time.Minute),
		backoff.WithJitterFactor(0.1))

	query, err := gojq.Parse(fmt.Sprintf(`%s`, opts.Url))
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
	if opts.YamlOutput {
		enc = yaml.NewEncoder(os.Stdout)
	} else {
		enc = json.NewEncoder(os.Stdout)
	}

	sem := semaphore.NewWeighted(opts.Parallelism)
	var muStderr sync.Mutex
	var muStdout sync.Mutex

	var dec interface {
		Decode(interface{}) error
	}

	if opts.RawInput {
		dec = &lineDecoder{bufio.NewScanner(os.Stdin)}
	} else if opts.YamlInput {
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
			if opts.CollectionName != "" {
				collectionName = opts.CollectionName
			} else if opts.AutoCollection {
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
					if opts.BillingProject != "" {
						req.Header.Add("x-goog-user-project", opts.BillingProject)
					}
					if !opts.Execute || opts.Verbose {
						muStderr.Lock()
						log.Printf("do url[%v]: %v %v\n", nowCount, req.Method, req.URL.String())
						muStderr.Unlock()
					}
					if !opts.Execute {
						return nil
					}

					backoffCtl := backoffPolicy.Start(ctx)
					resp, err := func() (*http.Response, error) {
						for backoff.Continue(backoffCtl) {
							resp, err := func() (*http.Response, error) {
								var buf bytes.Buffer
								if opts.LogHttp {
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
							} else if resp.StatusCode == http.StatusTooManyRequests {
								log.Printf("retry url[%v]: %v %v, reason: %v\n", nowCount, resp.Request.Method, resp.Request.URL.String(), resp.Status)
								continue
							} else if resp.StatusCode >= 400 && resp.StatusCode < 500 {
								log.Printf("error url[%v]: %v %v, reason: %v\n", nowCount, resp.Request.Method, resp.Request.URL.String(), resp.Status)
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

					if !opts.IncludeError && resp.StatusCode != http.StatusOK {
						return nil
					}

					if collectionName == "" || resp.StatusCode != http.StatusOK {
						result = output{
							Input:    input,
							Response: i,
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
						Input:    input,
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
