package main

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"time"
)

// objectsCmd represents the objects command
var objectsCmd = &cobra.Command{
	Use:              "objects",
	Short:            "Find, add and remove storage objects",
	Long:             ``,
	SilenceUsage:     true,
	TraverseChildren: true,
}

func init() {
	rootCmd.AddCommand(objectsCmd)
	objectsCmd.AddCommand(listObjectsCmd())
	objectsCmd.AddCommand(getObjectsCmd())
	objectsCmd.AddCommand(putObjectsCmd())
	objectsCmd.AddCommand(deleteObjectsCmd())
	objectsCmd.AddCommand(syncObjectsCmd())

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// objectsCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// objectsCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func listObjectsCmd() *cobra.Command {
	var listObjectsCmd = &cobra.Command{
		Use:   "list",
		Short: "List storage objects",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			handleOsSignal(func(signal os.Signal) {
				time.AfterFunc(15*time.Second, func() {
					logger.Fatalf("Failed to shutdown normally. Closed after 15 sec shutdown")
				})
			})
		},
		TraverseChildren: true,
	}

	addOutputFormatFlag(listObjectsCmd)

	return listObjectsCmd
}

func getObjectsCmd() *cobra.Command {
	var getObjectsCmd = &cobra.Command{
		Use:   "get",
		Short: "Get storage object info",
		Long:  ``,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("specify object path: `storage get <path>`")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			handleOsSignal(func(signal os.Signal) {
				time.AfterFunc(15*time.Second, func() {
					logger.Fatalf("Failed to shutdown normally. Closed after 15 sec shutdown")
				})
			})
		},
		TraverseChildren: true,
	}

	addOutputFormatFlag(getObjectsCmd)

	return getObjectsCmd
}

func putObjectsCmd() *cobra.Command {
	var putObjectsCmd = &cobra.Command{
		Use:   "put",
		Short: "Put storage object",
		Long:  ``,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("specify object path: `storage put <path>`")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			handleOsSignal(func(signal os.Signal) {
				time.AfterFunc(15*time.Second, func() {
					logger.Fatalf("Failed to shutdown normally. Closed after 15 sec shutdown")
				})
			})
		},
		TraverseChildren: true,
	}

	return putObjectsCmd
}

func deleteObjectsCmd() *cobra.Command {
	var deleteObjectsCmd = &cobra.Command{
		Use:   "delete",
		Short: "Removes storage object",
		Long:  ``,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("specify object path: `storage delete <path>`")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			handleOsSignal(func(signal os.Signal) {
				time.AfterFunc(15*time.Second, func() {
					logger.Fatalf("Failed to shutdown normally. Closed after 15 sec shutdown")
				})
			})
		},
		TraverseChildren: true,
	}

	return deleteObjectsCmd
}

func syncObjectsCmd() *cobra.Command {
	var syncObjectsCmd = &cobra.Command{
		Use:     "sync",
		Short:   "Sync storage objects",
		Long:    ``,
		Example: `storage objects sync /src/path /dst/path`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 2 {
				return fmt.Errorf("specify object path: `storage put <path>`")
			}

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			recursive, _ := cmd.Flags().GetBool("recursive")
			exlude, _ := cmd.Flags().GetStringArray("exclude")
			include, _ := cmd.Flags().GetStringArray("include")

			fmt.Println(recursive, exlude, include)

			handleOsSignal(func(signal os.Signal) {
				time.AfterFunc(15*time.Second, func() {
					logger.Fatalf("Failed to shutdown normally. Closed after 15 sec shutdown")
				})
			})

			logger.Warn("Not implemented")
		},
		TraverseChildren: true,
	}

	syncObjectsCmd.Flags().BoolP("recursive", "r", true, "Recursively sync all files")
	syncObjectsCmd.Flags().StringArray("exclude", []string{}, "Exclude specified paths")
	syncObjectsCmd.Flags().StringArray("include", []string{}, "Include only specified paths")

	return syncObjectsCmd
}
