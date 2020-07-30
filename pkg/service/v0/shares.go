package svc

import (
	"context"
	"encoding/base64"
	"fmt"
	"mime"
	"net/http"
	"path"
	"strconv"
	"time"

	revagateway "github.com/cs3org/go-cs3apis/cs3/gateway/v1beta1"
	revauser "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	revarpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	revacollaboration "github.com/cs3org/go-cs3apis/cs3/sharing/collaboration/v1beta1"
	revalink "github.com/cs3org/go-cs3apis/cs3/sharing/link/v1beta1"
	revaprovider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	revatypes "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"
	"github.com/cs3org/reva/pkg/appctx"
	"github.com/cs3org/reva/pkg/rgrpc/todo/pool"
	"github.com/go-chi/render"
	"github.com/owncloud/ocis-ocs/pkg/service/v0/data"
	"github.com/owncloud/ocis-ocs/pkg/service/v0/response"
	"github.com/pkg/errors"
)

const (
	stateAll      string = "all"
	stateAccepted string = "0"
	statePending  string = "1"
	stateRejected string = "2"

	ocsStateAccepted int = 0
	ocsStatePending  int = 1
	ocsStateRejected int = 2
)

// ListShares implements the endpoint to list shares
func (o Ocs) ListShares(w http.ResponseWriter, r *http.Request) {
	sharedWithMe, err := isSharedWithMe(r)
	if err != nil {
		render.Render(w, r, response.ErrRender(data.MetaServerError.StatusCode, err.Error()))
		return
	}

	gwc, err := pool.GetGatewayServiceClient("") // TODO put in gateway address
	if err != nil {
		render.Render(w, r, response.ErrRender(
			data.MetaServerError.StatusCode,
			fmt.Sprintf("error getting grpc gateway client %v", err),
		))
		return
	}
	if sharedWithMe {
		listSharesWithMe(w, r, gwc)
		return
	}
	listSharesWithOthers(w, r, gwc)
}

func isSharedWithMe(r *http.Request) (bool, error) {
	v := r.FormValue("shared_with_me")
	if v == "" {
		return false, nil
	}
	listSharedWithMe, err := strconv.ParseBool(v)
	if err != nil {
		return false, errors.Wrap(err, "error mapping share data")
	}
	return listSharedWithMe, nil
}

func listSharesWithMe(w http.ResponseWriter, r *http.Request, client revagateway.GatewayAPIClient) {
	switch r.FormValue("state") {
	default:
		fallthrough
	case stateAccepted:
		// TODO implement accepted filter
	case statePending:
		// TODO implement pending filter
	case stateRejected:
		// TODO implement rejected filter
	case stateAll:
		// no filter
	}

	lrsReq := revacollaboration.ListReceivedSharesRequest{}
	lrsRes, err := client.ListReceivedShares(r.Context(), &lrsReq)
	if err != nil {
		render.Render(w, r, response.ErrRender(
			data.MetaServerError.StatusCode,
			fmt.Sprintf("error sending a grpc ListReceivedShares request %v", err),
		))
		return
	}

	if lrsRes.Status.Code != revarpc.Code_CODE_OK {
		if lrsRes.Status.Code == revarpc.Code_CODE_NOT_FOUND {
			render.Render(w, r, response.ErrRender(data.MetaNotFound.StatusCode, "not found"))
			return
		}
		render.Render(w, r, response.ErrRender(
			data.MetaServerError.StatusCode,
			fmt.Sprintf("grpc ListReceivedShares request failed %v", err),
		))
		return
	}

	shares := make([]*data.ShareData, 0)
	// TODO(refs) filter out "invalid" shares
	for _, rs := range lrsRes.GetShares() {
		statRequest := revaprovider.StatRequest{
			Ref: &revaprovider.Reference{
				Spec: &revaprovider.Reference_Id{
					Id: rs.Share.ResourceId,
				},
			},
		}

		statResponse, err := client.Stat(r.Context(), &statRequest)
		if err != nil {
			render.Render(w, r, response.ErrRender(data.MetaServerError.StatusCode, err.Error()))
			return
		}

		shareData, err := userShare2ShareData(r.Context(), rs.Share, client)
		if err != nil {
			render.Render(w, r, response.ErrRender(data.MetaServerError.StatusCode, err.Error()))
			return
		}

		switch rs.GetState() {
		case revacollaboration.ShareState_SHARE_STATE_PENDING:
			shareData.State = ocsStatePending
		case revacollaboration.ShareState_SHARE_STATE_ACCEPTED:
			shareData.State = ocsStateAccepted
		case revacollaboration.ShareState_SHARE_STATE_REJECTED:
			shareData.State = ocsStateRejected
		default:
			shareData.State = -1
		}

		err = addFileInfo(r.Context(), shareData, statResponse.Info, client)
		if err != nil {
			render.Render(w, r, response.ErrRender(data.MetaServerError.StatusCode, err.Error()))
			return
		}

		shares = append(shares, shareData)
	}

	render.Render(w, r, response.DataRender(shares))
}

// TODO(jfd) merge userShare2ShareData with publicShare2ShareData
func userShare2ShareData(ctx context.Context, share *revacollaboration.Share, client revagateway.GatewayAPIClient) (*data.ShareData, error) {
	sd := &data.ShareData{
		Permissions: userSharePermissions2OCSPermissions(share.GetPermissions()),
		ShareType:   data.ShareTypeUser,
	}

	log := appctx.GetLogger(ctx)

	if share.Creator != nil {
		creator, err := client.GetUser(ctx, &revauser.GetUserRequest{
			UserId: share.Creator,
		})
		if err != nil {
			return nil, err
		}

		if creator.Status.Code == revarpc.Code_CODE_OK {
			// TODO the user from GetUser might not have an ID set, so we are using the one we have
			sd.UIDOwner = userIDToString(share.Creator)
			sd.DisplaynameOwner = creator.GetUser().DisplayName
		} else {
			log.Err(errors.Wrap(err, "could not look up creator")).
				Str("user_idp", share.Creator.GetIdp()).
				Str("user_opaque_id", share.Creator.GetOpaqueId()).
				Str("code", creator.Status.Code.String()).
				Msg(creator.Status.Message)
			return nil, err
		}
	}
	if share.Owner != nil {
		owner, err := client.GetUser(ctx, &revauser.GetUserRequest{
			UserId: share.Owner,
		})
		if err != nil {
			return nil, err
		}

		if owner.Status.Code == revarpc.Code_CODE_OK {
			// TODO the user from GetUser might not have an ID set, so we are using the one we have
			sd.UIDFileOwner = userIDToString(share.Owner)
			sd.DisplaynameFileOwner = owner.GetUser().DisplayName
		} else {
			log.Err(errors.Wrap(err, "could not look up owner")).
				Str("user_idp", share.Owner.GetIdp()).
				Str("user_opaque_id", share.Owner.GetOpaqueId()).
				Str("code", owner.Status.Code.String()).
				Msg(owner.Status.Message)
			return nil, err
		}
	}
	if share.Grantee.Id != nil {
		grantee, err := client.GetUser(ctx, &revauser.GetUserRequest{
			UserId: share.Grantee.GetId(),
		})
		if err != nil {
			return nil, err
		}

		if grantee.Status.Code == revarpc.Code_CODE_OK {
			// TODO the user from GetUser might not have an ID set, so we are using the one we have
			sd.ShareWith = userIDToString(share.Grantee.Id)
			sd.ShareWithDisplayname = grantee.GetUser().DisplayName
		} else {
			log.Err(errors.Wrap(err, "could not look up grantee")).
				Str("user_idp", share.Grantee.GetId().GetIdp()).
				Str("user_opaque_id", share.Grantee.GetId().GetOpaqueId()).
				Str("code", grantee.Status.Code.String()).
				Msg(grantee.Status.Message)
			return nil, err
		}
	}
	if share.Id != nil && share.Id.OpaqueId != "" {
		sd.ID = share.Id.OpaqueId
	}
	if share.Ctime != nil {
		sd.STime = share.Ctime.Seconds // TODO CS3 api birth time = btime
	}
	// actually clients should be able to GET and cache the user info themselves ...
	// TODO check grantee type for user vs group
	return sd, nil
}

// UserSharePermissions2OCSPermissions transforms cs3api permissions into OCS Permissions data model
func userSharePermissions2OCSPermissions(sp *revacollaboration.SharePermissions) data.Permissions {
	if sp != nil {
		return permissions2OCSPermissions(sp.GetPermissions())
	}
	return data.PermissionInvalid
}

// TODO sort out mapping, this is just a first guess
// public link permissions to OCS permissions
func permissions2OCSPermissions(p *revaprovider.ResourcePermissions) data.Permissions {
	permissions := data.PermissionInvalid
	if p != nil {
		if p.ListContainer {
			permissions += data.PermissionRead
		}
		if p.InitiateFileUpload {
			permissions += data.PermissionWrite
		}
		if p.CreateContainer {
			permissions += data.PermissionCreate
		}
		if p.Delete {
			permissions += data.PermissionDelete
		}
		if p.AddGrant {
			permissions += data.PermissionShare
		}
	}
	return permissions
}

func userIDToString(userID *revauser.UserId) string {
	if userID == nil || userID.OpaqueId == "" {
		return ""
	}
	return userID.OpaqueId
}

func addFileInfo(ctx context.Context, s *data.ShareData, info *revaprovider.ResourceInfo, client revagateway.GatewayAPIClient) error {
	log := appctx.GetLogger(ctx)
	if info != nil {
		// TODO The owner is not set in the storage stat metadata ...
		parsedMt, _, err := mime.ParseMediaType(info.MimeType)
		if err != nil {
			// Should never happen. We log anyways so that we know if it happens.
			log.Warn().Err(err).Msg("failed to parse mimetype")
		}
		s.MimeType = parsedMt
		// TODO STime:     &types.Timestamp{Seconds: info.Mtime.Seconds, Nanos: info.Mtime.Nanos},
		s.StorageID = info.Id.StorageId
		// TODO Storage: int
		s.ItemSource = wrapResourceID(info.Id)
		s.FileSource = s.ItemSource
		s.FileTarget = path.Join("/", path.Base(info.Path))
		s.Path = path.Join("/", path.Base(info.Path)) // TODO hm this might have to be relative to the users home ... depends on the webdav_namespace config
		// TODO FileParent:
		// item type
		s.ItemType = data.ResourceType(info.GetType()).String()

		// file owner might not yet be set. Use file info
		if s.UIDFileOwner == "" {
			// TODO we don't know if info.Owner is always set.
			s.UIDFileOwner = userIDToString(info.Owner)
		}
		if s.DisplaynameFileOwner == "" && info.Owner != nil {
			owner, err := client.GetUser(ctx, &revauser.GetUserRequest{
				UserId: info.Owner,
			})
			if err != nil {
				return err
			}

			if owner.Status.Code == revarpc.Code_CODE_OK {
				// TODO the user from GetUser might not have an ID set, so we are using the one we have
				s.DisplaynameFileOwner = owner.GetUser().DisplayName
			} else {
				err := errors.New("could not look up share owner")
				log.Err(err).
					Str("user_idp", info.Owner.GetIdp()).
					Str("user_opaque_id", info.Owner.GetOpaqueId()).
					Str("code", owner.Status.Code.String()).
					Msg(owner.Status.Message)
				return err
			}
		}
		// share owner might not yet be set. Use file info
		if s.UIDOwner == "" {
			// TODO we don't know if info.Owner is always set.
			s.UIDOwner = userIDToString(info.Owner)
		}
		if s.DisplaynameOwner == "" && info.Owner != nil {
			owner, err := client.GetUser(ctx, &revauser.GetUserRequest{
				UserId: info.Owner,
			})

			if err != nil {
				return err
			}

			if owner.Status.Code == revarpc.Code_CODE_OK {
				// TODO the user from GetUser might not have an ID set, so we are using the one we have
				s.DisplaynameOwner = owner.User.DisplayName
			} else {
				err := errors.New("could not look up file owner")
				log.Err(err).
					Str("user_idp", info.Owner.GetIdp()).
					Str("user_opaque_id", info.Owner.GetOpaqueId()).
					Str("code", owner.Status.Code.String()).
					Msg(owner.Status.Message)
				return err
			}
		}
	}
	return nil
}

func wrapResourceID(r *revaprovider.ResourceId) string {
	return wrap(r.StorageId, r.OpaqueId)
}

// The fileID must be encoded
// - XML safe, because it is going to be used in the propfind result
// - url safe, because the id might be used in a url, eg. the /dav/meta nodes
// which is why we base64 encode it
func wrap(sid string, oid string) string {
	return base64.URLEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", sid, oid)))
}

func listSharesWithOthers(w http.ResponseWriter, r *http.Request, client revagateway.GatewayAPIClient) {
	filters := []*revacollaboration.ListSharesRequest_Filter{}
	linkFilters := []*revalink.ListPublicSharesRequest_Filter{}

	p := r.URL.Query().Get("path")
	if p != "" {
		hRes, err := client.GetHome(r.Context(), &revaprovider.GetHomeRequest{})
		if err != nil {
			render.Render(w, r, response.ErrRender(
				data.MetaServerError.StatusCode,
				fmt.Sprintf("error sending a grpc get home request %v", err),
			))
			return
		}

		filters, linkFilters, err = addFilters(r.Context(), hRes.GetPath(), r.FormValue("path"), client)
		if err != nil {
			render.Render(w, r, response.ErrRender(data.MetaServerError.StatusCode, err.Error()))
			return
		}
	}

	userShares, err := listUserShares(r.Context(), filters, client)
	if err != nil {
		render.Render(w, r, response.ErrRender(data.MetaBadRequest.StatusCode, err.Error()))
		return
	}

	publicShares, err := listPublicShares(r.Context(), linkFilters, client)
	if err != nil {
		render.Render(w, r, response.ErrRender(data.MetaServerError.StatusCode, err.Error()))
		return
	}

	shares := append(userShares, publicShares...)

	render.Render(w, r, response.DataRender(shares))
}

func addFilters(ctx context.Context, prefix string, targetPath string, client revagateway.GatewayAPIClient) ([]*revacollaboration.ListSharesRequest_Filter, []*revalink.ListPublicSharesRequest_Filter, error) {
	collaborationFilters := []*revacollaboration.ListSharesRequest_Filter{}
	linkFilters := []*revalink.ListPublicSharesRequest_Filter{}

	target := path.Join(prefix, targetPath)

	statReq := &revaprovider.StatRequest{
		Ref: &revaprovider.Reference{
			Spec: &revaprovider.Reference_Path{
				Path: target,
			},
		},
	}

	res, err := client.Stat(ctx, statReq)
	if err != nil {
		return nil, nil, errors.Wrap(err, "error sending a grpc stat request")
	}

	if res.Status.Code != revarpc.Code_CODE_OK {
		if res.Status.Code == revarpc.Code_CODE_NOT_FOUND {
			return nil, nil, errors.Wrap(err, "not found")
		}
		return nil, nil, errors.Wrap(err, "grpc stat request failed")
	}

	info := res.Info

	collaborationFilters = append(collaborationFilters, &revacollaboration.ListSharesRequest_Filter{
		Type: revacollaboration.ListSharesRequest_Filter_TYPE_RESOURCE_ID,
		Term: &revacollaboration.ListSharesRequest_Filter_ResourceId{
			ResourceId: info.Id,
		},
	})

	linkFilters = append(linkFilters, &revalink.ListPublicSharesRequest_Filter{
		Type: revalink.ListPublicSharesRequest_Filter_TYPE_RESOURCE_ID,
		Term: &revalink.ListPublicSharesRequest_Filter_ResourceId{
			ResourceId: info.Id,
		},
	})

	return collaborationFilters, linkFilters, nil
}

func listUserShares(ctx context.Context, filters []*revacollaboration.ListSharesRequest_Filter, client revagateway.GatewayAPIClient) ([]*data.ShareData, error) {
	lsUserSharesRequest := revacollaboration.ListSharesRequest{
		Filters: filters,
	}

	ocsDataPayload := make([]*data.ShareData, 0)

	lsUserSharesResponse, err := client.ListShares(ctx, &lsUserSharesRequest)
	if err != nil || lsUserSharesResponse.Status.Code != revarpc.Code_CODE_OK {
		return nil, errors.Wrap(err, "could not list shares")
	}

	for _, s := range lsUserSharesResponse.Shares {
		share, err := userShare2ShareData(ctx, s, client)
		if err != nil {
			return nil, errors.Wrap(err, "could not map user share to sharedata")
		}

		statReq := &revaprovider.StatRequest{
			Ref: &revaprovider.Reference{
				Spec: &revaprovider.Reference_Id{Id: s.ResourceId},
			},
		}

		statResponse, err := client.Stat(ctx, statReq)
		if err != nil || statResponse.Status.Code != revarpc.Code_CODE_OK {
			return nil, errors.Wrap(err, "could not stat share target")
		}

		err = addFileInfo(ctx, share, statResponse.Info, client)
		if err != nil {
			return nil, errors.Wrap(err, "could not add file info to share")
		}

		ocsDataPayload = append(ocsDataPayload, share)
	}

	return ocsDataPayload, nil
}

func listPublicShares(ctx context.Context, filters []*revalink.ListPublicSharesRequest_Filter, client revagateway.GatewayAPIClient) ([]*data.ShareData, error) {
	req := revalink.ListPublicSharesRequest{
		Filters: filters,
	}

	res, err := client.ListPublicShares(ctx, &req)
	if err != nil {
		return nil, errors.Wrap(err, "could not list public shares")
	}

	ocsDataPayload := make([]*data.ShareData, 0)
	for _, share := range res.GetShare() {
		statRequest := &revaprovider.StatRequest{
			Ref: &revaprovider.Reference{
				Spec: &revaprovider.Reference_Id{
					Id: share.ResourceId,
				},
			},
		}

		statResponse, err := client.Stat(ctx, statRequest)
		if err != nil || statResponse.Status.Code != revarpc.Code_CODE_OK {
			return nil, errors.Wrap(err, "could not stat share target")
		}

		sData := publicShare2ShareData(share, "") // TODO pass publicURL

		sData.Name = share.DisplayName

		if addFileInfo(ctx, sData, statResponse.Info, client) != nil {
			return nil, errors.Wrap(err, "could not add file info")
		}

		ocsDataPayload = append(ocsDataPayload, sData)
	}
	return ocsDataPayload, nil
}

func publicShare2ShareData(share *revalink.PublicShare, publicURL string) *data.ShareData {
	var expiration string
	if share.Expiration != nil {
		expiration = timestampToExpiration(share.Expiration)
	} else {
		expiration = ""
	}

	shareWith := ""
	if share.PasswordProtected {
		shareWith = "***redacted***"
	}

	return &data.ShareData{
		// share.permissions are mapped below
		// DisplaynameOwner:	 creator.DisplayName,
		// DisplaynameFileOwner: share.GetCreator().String(),
		ID:                   share.Id.OpaqueId,
		ShareType:            data.ShareTypePublicLink,
		ShareWith:            shareWith,
		ShareWithDisplayname: shareWith,
		STime:                share.Ctime.Seconds, // TODO CS3 api birth time = btime
		Token:                share.Token,
		Expiration:           expiration,
		MimeType:             share.Mtime.String(),
		Name:                 share.DisplayName,
		MailSend:             0,
		URL:                  publicURL + path.Join("/", "#/s/"+share.Token),
		Permissions:          publicSharePermissions2OCSPermissions(share.GetPermissions()),
		UIDOwner:             userIDToString(share.Creator),
		UIDFileOwner:         userIDToString(share.Owner),
	}
	// actually clients should be able to GET and cache the user info themselves ...
	// TODO check grantee type for user vs group
}

func publicSharePermissions2OCSPermissions(sp *revalink.PublicSharePermissions) data.Permissions {
	if sp != nil {
		return permissions2OCSPermissions(sp.GetPermissions())
	}
	return data.PermissionInvalid
}

// timestamp is assumed to be UTC ... just human readable ...
// FIXME and abiguous / error prone because there is no time zone ...
func timestampToExpiration(t *revatypes.Timestamp) string {
	return time.Unix(int64(t.Seconds), int64(t.Nanos)).UTC().Format("2006-01-02 15:05:05")
}
