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

package hnsw

import "github.com/weaviate/weaviate/entities/vectorindex/common"

const (
	DefaultSQEnabled = false
)

type SQConfig struct {
	Enabled bool `json:"enabled"`
}

func parseSQMap(in map[string]interface{}, sq *SQConfig) error {
	sqConfigValue, ok := in["sq"]
	if !ok {
		return nil
	}

	sqConfigMap, ok := sqConfigValue.(map[string]interface{})
	if !ok {
		return nil
	}

	if err := common.OptionalBoolFromMap(sqConfigMap, "enabled", func(v bool) {
		sq.Enabled = v
	}); err != nil {
		return err
	}

	return nil
}
