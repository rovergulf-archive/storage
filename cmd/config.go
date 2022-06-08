package main

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// configCmd represents the config command
var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Shows config command available options",
	Long:  ``,
	//Run: func(cmd *cobra.Command, args []string) {
	//	fmt.Println("config called")
	//},
	SilenceUsage: true,
}

func init() {
	rootCmd.AddCommand(configCmd)
	configCmd.AddCommand(configGetCmd())
}

// configGetCmd represents the config get command
func configGetCmd() *cobra.Command {
	var configGetCmd = &cobra.Command{
		Use:   "get",
		Short: "Get value of config property",
		Long: `Usage:
monkey config get <prop>`,
		TraverseChildren: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				return writeOutput(cmd, viper.Get(args[0]))
			} else {
				return writeOutput(cmd, viper.AllSettings())
			}
		},
	}

	addOutputFormatFlag(configGetCmd)

	return configGetCmd
}
