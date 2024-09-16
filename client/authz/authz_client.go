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

// Code generated by go-swagger; DO NOT EDIT.

package authz

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
)

// New creates a new authz API client.
func New(transport runtime.ClientTransport, formats strfmt.Registry) ClientService {
	return &Client{transport: transport, formats: formats}
}

/*
Client for authz API
*/
type Client struct {
	transport runtime.ClientTransport
	formats   strfmt.Registry
}

// ClientOption is the option for Client methods
type ClientOption func(*runtime.ClientOperation)

// ClientService is the interface for Client methods
type ClientService interface {
	AddPolicy(params *AddPolicyParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*AddPolicyCreated, error)

	AddRole(params *AddRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*AddRoleOK, error)

	DeletePolicy(params *DeletePolicyParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*DeletePolicyNoContent, error)

	GetPolicies(params *GetPoliciesParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*GetPoliciesOK, error)

	GetRole(params *GetRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*GetRoleOK, error)

	RemoveRole(params *RemoveRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*RemoveRoleOK, error)

	SetTransport(transport runtime.ClientTransport)
}

/*
AddPolicy adds a new policy
*/
func (a *Client) AddPolicy(params *AddPolicyParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*AddPolicyCreated, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewAddPolicyParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "addPolicy",
		Method:             "POST",
		PathPattern:        "/authz/policy",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &AddPolicyReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	}
	for _, opt := range opts {
		opt(op)
	}

	result, err := a.transport.Submit(op)
	if err != nil {
		return nil, err
	}
	success, ok := result.(*AddPolicyCreated)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for addPolicy: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
AddRole assigns a role to a user
*/
func (a *Client) AddRole(params *AddRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*AddRoleOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewAddRoleParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "addRole",
		Method:             "POST",
		PathPattern:        "/authz/role",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &AddRoleReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	}
	for _, opt := range opts {
		opt(op)
	}

	result, err := a.transport.Submit(op)
	if err != nil {
		return nil, err
	}
	success, ok := result.(*AddRoleOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for addRole: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
DeletePolicy deletes a policy
*/
func (a *Client) DeletePolicy(params *DeletePolicyParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*DeletePolicyNoContent, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewDeletePolicyParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "deletePolicy",
		Method:             "DELETE",
		PathPattern:        "/authz/policies/{id}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &DeletePolicyReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	}
	for _, opt := range opts {
		opt(op)
	}

	result, err := a.transport.Submit(op)
	if err != nil {
		return nil, err
	}
	success, ok := result.(*DeletePolicyNoContent)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for deletePolicy: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
GetPolicies gets all policies
*/
func (a *Client) GetPolicies(params *GetPoliciesParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*GetPoliciesOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewGetPoliciesParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "getPolicies",
		Method:             "GET",
		PathPattern:        "/authz/policies",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &GetPoliciesReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	}
	for _, opt := range opts {
		opt(op)
	}

	result, err := a.transport.Submit(op)
	if err != nil {
		return nil, err
	}
	success, ok := result.(*GetPoliciesOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for getPolicies: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
GetRole gets roles for user or a key or users for specified role
*/
func (a *Client) GetRole(params *GetRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*GetRoleOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewGetRoleParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "getRole",
		Method:             "GET",
		PathPattern:        "/authz/role",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &GetRoleReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	}
	for _, opt := range opts {
		opt(op)
	}

	result, err := a.transport.Submit(op)
	if err != nil {
		return nil, err
	}
	success, ok := result.(*GetRoleOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for getRole: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
RemoveRole removes a role from a user
*/
func (a *Client) RemoveRole(params *RemoveRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*RemoveRoleOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewRemoveRoleParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "removeRole",
		Method:             "DELETE",
		PathPattern:        "/authz/role",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &RemoveRoleReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	}
	for _, opt := range opts {
		opt(op)
	}

	result, err := a.transport.Submit(op)
	if err != nil {
		return nil, err
	}
	success, ok := result.(*RemoveRoleOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for removeRole: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

// SetTransport changes the transport on the client
func (a *Client) SetTransport(transport runtime.ClientTransport) {
	a.transport = transport
}
