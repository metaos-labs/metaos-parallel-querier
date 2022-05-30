package sophon_parallel_querier

import (
	"context"
	"fmt"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
	grpctypes "github.com/cosmos/cosmos-sdk/types/grpc"
	"github.com/cosmos/cosmos-sdk/types/tx"
	"github.com/gogo/protobuf/grpc"
	"github.com/tendermint/tendermint/abci/types"
	grpc2 "google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/metadata"
	"reflect"
	"strconv"
)

var _ grpc.ClientConn = ParallelQuerier{}

var protoCodec = encoding.GetCodec(proto.Name)

type ParallelQuerier struct {
	app types.Application
	ir  codectypes.InterfaceRegistry
}

func NewParallelQuerier(app types.Application, ir codectypes.InterfaceRegistry) *ParallelQuerier {
	return &ParallelQuerier{
		app: app,
		ir:  ir,
	}
}

// Invoke See client.Context
func (p ParallelQuerier) Invoke(
	grpcCtx context.Context, method string, req, reply interface{},
	opts ...grpc2.CallOption,
) error {
	if reflect.ValueOf(req).IsNil() {
		return sdkerrors.Wrap(sdkerrors.ErrInvalidRequest, "request cannot be nil")
	}

	// Case1. Broadcasting a Tx.
	if _, ok := req.(*tx.BroadcastTxRequest); ok {
		return sdkerrors.Wrap(sdkerrors.ErrInvalidRequest, "unsupported to broadcast a tx")
	}

	// Case2. Querying state.
	reqBz, err := protoCodec.Marshal(req)
	if err != nil {
		return err
	}

	// parse height header
	md, _ := metadata.FromOutgoingContext(grpcCtx)
	var height int64 = 0
	if heights := md.Get(grpctypes.GRPCBlockHeightHeader); len(heights) > 0 {
		height, err = strconv.ParseInt(heights[0], 10, 64)
		if err != nil {
			return err
		}
		if height < 0 {
			return sdkerrors.Wrapf(sdkerrors.ErrInvalidRequest,
				"ParallelQuerier.Invoke: height (%d) form %q must be >= 0", height, grpctypes.GRPCBlockHeightHeader)
		}
	}
	abciReq := types.RequestQuery{
		Path:   method,
		Data:   reqBz,
		Height: height,
	}
	res := p.app.Query(abciReq)
	err = protoCodec.Unmarshal(res.Value, reply)
	if err != nil {
		return err
	}

	md = metadata.Pairs(grpctypes.GRPCBlockHeightHeader, strconv.FormatInt(res.Height, 10))
	for _, callOpt := range opts {
		header, ok := callOpt.(grpc2.HeaderCallOption)
		if !ok {
			continue
		}

		*header.HeaderAddr = md
	}

	if p.ir != nil {
		return codectypes.UnpackInterfaces(reply, p.ir)
	}
	return nil
}

func (p ParallelQuerier) NewStream(context.Context, *grpc2.StreamDesc, string, ...grpc2.CallOption) (grpc2.ClientStream, error) {
	return nil, fmt.Errorf("streaming rpc not supported")
}
