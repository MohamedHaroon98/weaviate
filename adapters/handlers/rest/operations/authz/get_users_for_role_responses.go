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
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/entities/models"
)

// GetUsersForRoleOKCode is the HTTP code returned for type GetUsersForRoleOK
const GetUsersForRoleOKCode int = 200

/*
GetUsersForRoleOK Role assigned users

swagger:response getUsersForRoleOK
*/
type GetUsersForRoleOK struct {

	/*
	  In: Body
	*/
	Payload []string `json:"body,omitempty"`
}

// NewGetUsersForRoleOK creates GetUsersForRoleOK with default headers values
func NewGetUsersForRoleOK() *GetUsersForRoleOK {

	return &GetUsersForRoleOK{}
}

// WithPayload adds the payload to the get users for role o k response
func (o *GetUsersForRoleOK) WithPayload(payload []string) *GetUsersForRoleOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the get users for role o k response
func (o *GetUsersForRoleOK) SetPayload(payload []string) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *GetUsersForRoleOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	payload := o.Payload
	if payload == nil {
		// return empty array
		payload = make([]string, 0, 50)
	}

	if err := producer.Produce(rw, payload); err != nil {
		panic(err) // let the recovery middleware deal with this
	}
}

// GetUsersForRoleBadRequestCode is the HTTP code returned for type GetUsersForRoleBadRequest
const GetUsersForRoleBadRequestCode int = 400

/*
GetUsersForRoleBadRequest Bad request

swagger:response getUsersForRoleBadRequest
*/
type GetUsersForRoleBadRequest struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewGetUsersForRoleBadRequest creates GetUsersForRoleBadRequest with default headers values
func NewGetUsersForRoleBadRequest() *GetUsersForRoleBadRequest {

	return &GetUsersForRoleBadRequest{}
}

// WithPayload adds the payload to the get users for role bad request response
func (o *GetUsersForRoleBadRequest) WithPayload(payload *models.ErrorResponse) *GetUsersForRoleBadRequest {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the get users for role bad request response
func (o *GetUsersForRoleBadRequest) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *GetUsersForRoleBadRequest) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(400)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// GetUsersForRoleUnauthorizedCode is the HTTP code returned for type GetUsersForRoleUnauthorized
const GetUsersForRoleUnauthorizedCode int = 401

/*
GetUsersForRoleUnauthorized Unauthorized or invalid credentials.

swagger:response getUsersForRoleUnauthorized
*/
type GetUsersForRoleUnauthorized struct {
}

// NewGetUsersForRoleUnauthorized creates GetUsersForRoleUnauthorized with default headers values
func NewGetUsersForRoleUnauthorized() *GetUsersForRoleUnauthorized {

	return &GetUsersForRoleUnauthorized{}
}

// WriteResponse to the client
func (o *GetUsersForRoleUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// GetUsersForRoleForbiddenCode is the HTTP code returned for type GetUsersForRoleForbidden
const GetUsersForRoleForbiddenCode int = 403

/*
GetUsersForRoleForbidden Forbidden

swagger:response getUsersForRoleForbidden
*/
type GetUsersForRoleForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewGetUsersForRoleForbidden creates GetUsersForRoleForbidden with default headers values
func NewGetUsersForRoleForbidden() *GetUsersForRoleForbidden {

	return &GetUsersForRoleForbidden{}
}

// WithPayload adds the payload to the get users for role forbidden response
func (o *GetUsersForRoleForbidden) WithPayload(payload *models.ErrorResponse) *GetUsersForRoleForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the get users for role forbidden response
func (o *GetUsersForRoleForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *GetUsersForRoleForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// GetUsersForRoleNotFoundCode is the HTTP code returned for type GetUsersForRoleNotFound
const GetUsersForRoleNotFoundCode int = 404

/*
GetUsersForRoleNotFound no role found for user/key

swagger:response getUsersForRoleNotFound
*/
type GetUsersForRoleNotFound struct {
}

// NewGetUsersForRoleNotFound creates GetUsersForRoleNotFound with default headers values
func NewGetUsersForRoleNotFound() *GetUsersForRoleNotFound {

	return &GetUsersForRoleNotFound{}
}

// WriteResponse to the client
func (o *GetUsersForRoleNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(404)
}

// GetUsersForRoleInternalServerErrorCode is the HTTP code returned for type GetUsersForRoleInternalServerError
const GetUsersForRoleInternalServerErrorCode int = 500

/*
GetUsersForRoleInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response getUsersForRoleInternalServerError
*/
type GetUsersForRoleInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewGetUsersForRoleInternalServerError creates GetUsersForRoleInternalServerError with default headers values
func NewGetUsersForRoleInternalServerError() *GetUsersForRoleInternalServerError {

	return &GetUsersForRoleInternalServerError{}
}

// WithPayload adds the payload to the get users for role internal server error response
func (o *GetUsersForRoleInternalServerError) WithPayload(payload *models.ErrorResponse) *GetUsersForRoleInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the get users for role internal server error response
func (o *GetUsersForRoleInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *GetUsersForRoleInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
