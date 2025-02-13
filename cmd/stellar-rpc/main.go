package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	supportlog "github.com/stellar/go/support/log"
	goxdr "github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon"
)

func main() {
	var cfg config.Config

	rootCmd := &cobra.Command{
		Use:   "stellar-rpc",
		Short: "Start the remote stellar-rpc server",
		Run: func(_ *cobra.Command, _ []string) {
			if err := cfg.SetValues(os.LookupEnv); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			if err := cfg.Validate(); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			daemon.MustNew(&cfg, supportlog.New()).Run()
		},
	}

	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print version information and exit",
		Run: func(_ *cobra.Command, _ []string) {
			if config.CommitHash == "" {
				//nolint:forbidigo
				fmt.Printf("stellar-rpc dev\n")
			} else {
				// avoid printing the branch for the main branch
				// ( since that's what the end-user would typically have )
				// but keep it for internal build ( so that we'll know from which branch it
				// was built )
				branch := config.Branch
				if branch == "main" {
					branch = ""
				}
				//nolint:forbidigo
				fmt.Printf("stellar-rpc %s (%s) %s\n", config.Version, config.CommitHash, branch)
			}
			//nolint:forbidigo
			fmt.Printf("stellar-xdr %s\n", goxdr.CommitHash)
		},
	}

	genConfigFileCmd := &cobra.Command{
		Use:   "gen-config-file",
		Short: "Generate a config file with default settings",
		Run: func(_ *cobra.Command, _ []string) {
			// We can't call 'Validate' here because the config file we are
			// generating might not be complete. e.g. It might not include a network passphrase.
			if err := cfg.SetValues(os.LookupEnv); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			out, err := cfg.MarshalTOML()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			fmt.Println(string(out))
		},
	}

	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(genConfigFileCmd)

	if err := cfg.AddFlags(rootCmd); err != nil {
		fmt.Fprintf(os.Stderr, "could not parse config options: %v\n", err)
		os.Exit(1)
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "could not run: %v\n", err)

		os.Exit(1)
	}
}
