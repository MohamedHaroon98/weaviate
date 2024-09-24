//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package traverser

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/config"
)

func (t *Traverser) GetClass(ctx context.Context, principal *models.Principal,
	params dto.GetParams,
) ([]interface{}, error) {
	before := time.Now()

	ok := t.ratelimiter.TryInc()
	if !ok {
		// we currently have no concept of error status code or typed errors in
		// GraphQL, so there is no other way then to send a message containing what
		// we want to convey
		return nil, enterrors.NewErrRateLimit()
	}

	defer t.ratelimiter.Dec()

	t.metrics.QueriesGetInc(params.ClassName)
	defer t.metrics.QueriesGetDec(params.ClassName)
	defer t.metrics.QueriesObserveDuration(params.ClassName, before.UnixMilli())

	err := t.authorizer.Authorize(principal, "get", "traversal/*")
	if err != nil {
		return nil, err
	}

	if err := probeForRefDepthLimit(params.Properties); err != nil {
		return nil, err
	}

	unlock, err := t.locks.LockConnector()
	if err != nil {
		return nil, enterrors.NewErrLockConnector(err)
	}
	defer unlock()

	certainty := ExtractCertaintyFromParams(params)
	if certainty != 0 || params.AdditionalProperties.Certainty {
		// if certainty is provided as input, we must ensure
		// that the vector index is configured to use cosine
		// distance
		if err := t.validateGetDistanceParams(params); err != nil {
			return nil, err
		}
	}

	return t.explorer.GetClass(ctx, params)
}

// probeForRefDepthLimit checks to ensure reference nesting depth doesn't exceed the limit
// provided by QUERY_MAX_REF_DEPTH
func probeForRefDepthLimit(props search.SelectProperties) error {
	var (
		determineDepth func(prop search.SelectProperties, depth int) int
		maxDepth       = func() int {
			if raw := os.Getenv("QUERY_MAX_REF_DEPTH"); raw != "" {
				depth, err := strconv.Atoi(raw)
				if err != nil {
					return config.DefaultQueryMaxRefDepth
				}
				return depth
			}
			return config.DefaultQueryMaxRefDepth
		}()
	)

	determineDepth = func(props search.SelectProperties, depth int) int {
		if depth > maxDepth || len(props) == 0 {
			return depth
		}

		prop := props[0]
		if len(prop.Refs) > 0 {
			depth++
			for _, refTarget := range prop.Refs {
				return determineDepth(refTarget.RefProperties, depth)
			}
		}

		return determineDepth(props[1:], depth)
	}

	if depth := determineDepth(props, 0); depth > maxDepth {
		return fmt.Errorf("nested references depth exceeds QUERY_MAX_REF_DEPTH (%d)", maxDepth)
	}
	return nil
}
