package sophon_parallel_querier

import (
	"fmt"
	"github.com/cosmos/cosmos-sdk/server/api"
	"github.com/cosmos/cosmos-sdk/server/config"
	"github.com/cosmos/cosmos-sdk/server/types"
	"github.com/cosmos/cosmos-sdk/telemetry"
	"github.com/spf13/cobra"
	"github.com/tendermint/starport/starport/pkg/cosmoscmd"
	"github.com/tendermint/tendermint/libs/log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	FlagApiParallelEnable  = "api.parallel.enable"
	FlagApiParallelAddress = "api.parallel.address"
)

var (
	apiParallelEnable  *bool
	apiParallelAddress *string
)

func CustomizeStartCmd() {
	cosmoscmd.CustomizeStartCmd(func(startCmd *cobra.Command) {
		CustomizeStartCmdFromCmd(startCmd)
	})
}

func CustomizeStartCmdFromCmd(startCmd *cobra.Command) {
	apiParallelEnable = startCmd.Flags().Bool(FlagApiParallelEnable, false, "Defines if Cosmos-sdk parallel REST server should be enabled")
	apiParallelAddress = startCmd.Flags().String(FlagApiParallelAddress, "tcp://0.0.0.0:2317", "the parallel REST server address to listen on")
}

func StartParallelServer(apiSvr *api.Server, apiConfig config.APIConfig, logger log.Logger, cb func(apiSvr *api.Server) error) (*api.Server, error) {
	if apiParallelEnable == nil {
		return nil, fmt.Errorf("you should invoke CustomizeStartCmd() before StartParallelServer()")
	}
	if *apiParallelEnable == false {
		return nil, nil
	}
	clientCtx := apiSvr.ClientCtx

	l := logger.With("module", "api-parallel-server")
	parallelApiSrv := api.New(clientCtx, l)
	// Register api routes
	err := cb(parallelApiSrv)
	if err != nil {
		return nil, err
	}

	errCh := make(chan error)
	go func() {
		err := parallelApiSrv.Start(config.Config{
			Telemetry: telemetry.Config{
				ServiceName:         "api-parallel-server",
				Enabled:             true,
				EnableHostname:      true,
				EnableHostnameLabel: true,
				EnableServiceLabel:  true,
			},
			API: config.APIConfig{
				Enable:             apiConfig.Enable,
				Swagger:            false,
				EnableUnsafeCORS:   apiConfig.EnableUnsafeCORS,
				Address:            *apiParallelAddress,
				MaxOpenConnections: apiConfig.MaxOpenConnections,
				RPCReadTimeout:     apiConfig.RPCReadTimeout,
				RPCWriteTimeout:    apiConfig.RPCWriteTimeout,
				RPCMaxBodyBytes:    apiConfig.RPCMaxBodyBytes,
			},
		})
		if err != nil {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return nil, err
	case <-time.After(types.ServerStartTime): // assume server started successfully
	}

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigs
		l.Debug("signal received", "signal", sig)
		parallelApiSrv.Close()
	}()
	return parallelApiSrv, nil
}
