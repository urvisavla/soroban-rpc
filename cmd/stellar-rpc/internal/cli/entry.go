// Package cli is the entry point shared by the stellar-rpc binary and the two
// per-mode wrappers. It picks the mode, builds that mode's command tree, and
// hands off to the daemon the way each mode's own main did before #1108.
package cli

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	v1config "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/config"
	v2config "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/version"
)

// Entry is one run of the stellar-rpc command line. A zero field takes the
// process default: os.Args[1:], os.LookupEnv, os.Stdout, or os.Stderr.
type Entry struct {
	Args      []string
	LookupEnv func(string) (string, bool)
	Stdout    io.Writer
	Stderr    io.Writer

	// ForcedMode skips the mode selection and hides --enable-rpcv2. The two
	// wrappers under cmd/stellar-rpc/rpcv1 and cmd/stellar-rpc/rpcv2 set it.
	// Zero selects the mode from the arguments and the environment.
	ForcedMode Mode

	// Tests replace the two daemon launches with recorders.
	startRPCv1 func(cfg *v1config.Config, logger *supportlog.Entry) error
	startRPCv2 func(configPath string, flags v2config.FlagOverrides) error
}

// genConfigFileUse is the subcommand that prints the mode's compiled defaults.
const genConfigFileUse = "gen-config-file"

// errLogged marks a failure that was already reported through the logger,
// so Run does not print it a second time.
var errLogged = errors.New("startup refused")

// Run runs the command line and returns the process exit code: 0 after a
// clean run or a signal, 1 on any failure.
func (e *Entry) Run() int {
	e.fillDefaults()

	mode := e.ForcedMode
	if mode == 0 {
		var err error
		if mode, err = SelectMode(e.Args, e.LookupEnv); err != nil {
			fmt.Fprintln(e.Stderr, err)
			return 1
		}
	}

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetOutput(e.Stderr)

	var runErr error
	root, err := e.rootCommand(mode, logger, &runErr)
	if err != nil {
		fmt.Fprintf(e.Stderr, "could not parse config options: %v\n", err)
		return 1
	}
	root.SetArgs(e.Args)
	root.SetOut(e.Stdout)
	root.SetErr(e.Stderr)
	if err := root.Execute(); err != nil {
		fmt.Fprintf(e.Stderr, "could not run: %v\n", err)
		return 1
	}
	if runErr != nil {
		if !errors.Is(runErr, errLogged) {
			fmt.Fprintln(e.Stderr, runErr)
		}
		return 1
	}
	return 0
}

func (e *Entry) fillDefaults() {
	if e.Args == nil {
		e.Args = os.Args[1:]
	}
	if e.LookupEnv == nil {
		e.LookupEnv = os.LookupEnv
	}
	if e.Stdout == nil {
		e.Stdout = os.Stdout
	}
	if e.Stderr == nil {
		e.Stderr = os.Stderr
	}
	if e.startRPCv1 == nil {
		e.startRPCv1 = startRPCv1Daemon
	}
	if e.startRPCv2 == nil {
		e.startRPCv2 = startRPCv2Daemon
	}
}

// rootCommand builds the command tree of one mode only, so --help shows one
// mode and a flag of the other mode is an unknown flag.
func (e *Entry) rootCommand(mode Mode, logger *supportlog.Entry, runErr *error) (*cobra.Command, error) {
	var root *cobra.Command
	var err error
	if mode == ModeRPCv2 {
		root = e.rpcv2Command(logger, runErr)
	} else {
		root, err = e.rpcv1Command(logger, runErr)
		if err != nil {
			return nil, err
		}
	}
	if e.ForcedMode == 0 {
		// SelectMode already read this flag from the raw arguments. It is
		// registered here so cobra accepts it and --help lists it.
		root.PersistentFlags().Bool(EnableRPCv2Flag, mode == ModeRPCv2,
			"run the rpcv2 full-history daemon instead of rpcv1 ("+EnableRPCv2Env+")")
	}
	root.AddCommand(versionCommand(mode))
	return root, nil
}

func versionCommand(mode Mode) *cobra.Command {
	cmd := version.NewCommand()
	cmd.PostRun = func(cmd *cobra.Command, _ []string) {
		fmt.Fprintf(cmd.OutOrStdout(), "mode: %s (%s)\n", mode, mode.howToSwitch())
	}
	return cmd
}

// configPath resolves the config file the same way in both modes: the flag,
// then STELLAR_RPC_CONFIG_PATH, then, in rpcv1 mode only, the old
// SOROBAN_RPC_CONFIG_PATH. An empty result means no file.
func (e *Entry) configPath(cmd *cobra.Command, mode Mode) (string, error) {
	if cmd.Flags().Changed(ConfigPathFlag) {
		return cmd.Flags().GetString(ConfigPathFlag)
	}
	if path, ok := e.LookupEnv(ConfigPathEnv); ok {
		return path, nil
	}
	if mode == ModeRPCv1 {
		if path, ok := e.LookupEnv(legacyConfigPathEnv); ok {
			return path, nil
		}
	}
	return "", nil
}
