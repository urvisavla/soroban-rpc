package cli

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"strings"
	"syscall"

	"github.com/pelletier/go-toml"
	"github.com/spf13/cobra"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	v1config "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/bench"
	v2config "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
)

func (e *Entry) rpcv2Command(logger *supportlog.Entry, runErr *error) *cobra.Command {
	var printRuntimeConfig bool

	root := &cobra.Command{
		Use:   "stellar-rpc",
		Short: "Run the full-history streaming ingestion daemon (rpcv2)",
		Run: func(cmd *cobra.Command, _ []string) {
			*runErr = e.runRPCv2(cmd, logger, printRuntimeConfig)
		},
	}
	root.Flags().String(ConfigPathFlag, "",
		"path to the full-history streaming daemon TOML config ("+ConfigPathEnv+")")
	root.Flags().BoolVar(&printRuntimeConfig, "print-runtime-config", false,
		"resolve the config (file, then any override flags, then compiled defaults) "+
			"and print it as TOML instead of running the daemon")
	// Every TOML key is also a flag named by its dotted path
	// (--storage.default_data_dir, --service.methods.getLedgers.queue_limit);
	// set flags override the file.
	v2config.BindFlags(root.Flags())

	genConfigFileCmd := &cobra.Command{
		Use:   genConfigFileUse,
		Short: "Generate a config file with default settings",
		Run: func(cmd *cobra.Command, _ []string) {
			*runErr = genRPCv2ConfigFile(cmd)
		},
	}
	root.AddCommand(genConfigFileCmd)
	root.AddCommand(bench.NewCommand())
	return root
}

func (e *Entry) runRPCv2(cmd *cobra.Command, logger *supportlog.Entry, printRuntimeConfig bool) error {
	path, err := e.configPath(cmd, ModeRPCv2)
	if err != nil {
		return err
	}
	if path == "" {
		return errors.New("rpcv2 mode needs a config file: pass --" + ConfigPathFlag + " or set " + ConfigPathEnv)
	}
	if printRuntimeConfig {
		cfg, err := v2config.LoadConfigWithFlags(path, cmd.Flags())
		if err != nil {
			return err
		}
		return printTOML(cmd, cfg)
	}
	e.logIgnoredRPCv1Env(logger)
	logger.WithField("mode", ModeRPCv2.String()).Info("starting in rpcv2 mode; " + ModeRPCv2.howToSwitch())
	return e.startRPCv2(path, cmd.Flags())
}

// logIgnoredRPCv1Env lists the rpcv1 environment variables that are set.
// rpcv2 reads none of them. The line is info, not warn: the stock
// /etc/default/stellar-rpc in the Debian package sets three of them, so every
// .deb operator in rpcv2 mode sees it.
func (e *Entry) logIgnoredRPCv1Env(logger *supportlog.Entry) {
	var ignored []string
	for _, name := range (&v1config.Config{}).EnvVarNames() {
		if _, ok := e.LookupEnv(name); ok {
			ignored = append(ignored, name)
		}
	}
	if len(ignored) == 0 {
		return
	}
	logger.WithField("ignored", strings.Join(ignored, " ")).
		Info("rpcv1 environment variables are ignored in rpcv2 mode")
}

func genRPCv2ConfigFile(cmd *cobra.Command) error {
	return printTOML(cmd, v2config.Config{}.WithDefaults())
}

func printTOML(cmd *cobra.Command, cfg v2config.Config) error {
	out, err := toml.Marshal(cfg)
	if err != nil {
		return err
	}
	fmt.Fprintln(cmd.OutOrStdout(), string(out))
	return nil
}

func startRPCv2Daemon(configPath string, flags v2config.FlagOverrides) error {
	// Cancel the daemon on SIGINT/SIGTERM for a clean shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return rpcv2.RunDaemon(ctx, configPath, flags)
}
