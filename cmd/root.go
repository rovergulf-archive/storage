package main

import (
	"fmt"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/rovergulf/storage"
	"github.com/rovergulf/storage/pkg/response"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

var (
	cfgFile string
	logger  *zap.SugaredLogger
	backend storage.Backend
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "storage",
	Short: "Object storage manager",
	Long:  `File-system and cloud-storages compatible driver`,
	//	Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", os.Getenv("CONFIG"), "Config file path (default is $HOME/.rovergulf/config.yaml)")
	rootCmd.PersistentFlags().Bool("log-json", false, "Enable JSON formatted logs output")
	rootCmd.PersistentFlags().Int("log-level", int(zapcore.DebugLevel), "Log level")

	// bind viper persistent flags
	viper.BindPFlag("log_json", rootCmd.PersistentFlags().Lookup("log-json"))
	viper.BindPFlag("log_level", rootCmd.PersistentFlags().Lookup("log-level"))

	initZapLogger()
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	//viper.SetEnvPrefix("STORAGE")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		viper.AddConfigPath(home)
		viper.SetConfigName("storage.yaml")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}

	setDefaultFlags()
}

func initZapLogger() {
	config := zap.NewDevelopmentConfig()
	config.Development = viper.GetBool("dev")
	config.DisableStacktrace = viper.GetBool("log_stacktrace")

	if logJson := viper.GetBool("log_json"); logJson {
		config.Encoding = "json"
	} else {
		config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}

	logLevel, ok := viper.Get("log_level").(int)
	if !ok {
		logLevel = int(zapcore.DebugLevel)
	}

	config.Level = zap.NewAtomicLevelAt(zapcore.Level(logLevel))
	l, err := config.Build()
	if err != nil {
		log.Fatalf("Failed to run zap logger: %s", err)
	}

	logger = l.Sugar()
}

func setDefaultFlags() {

	// storage
	viper.SetDefault("type", "dir")
	viper.SetDefault("path", "tmp")

	// etcd
	viper.SetDefault("etcd.endpoints", []string{os.Getenv("ETCD_ADDR")})
	viper.SetDefault("etcd.role", os.Getenv("ETCD_ROLE"))
	viper.SetDefault("etcd.password", os.Getenv("ETCD_PASSWORD"))
	viper.SetDefault("etcd.ssl.enabled", os.Getenv("ETCD_SSL_ENABLED"))
	viper.SetDefault("etcd.ssl.ca", os.Getenv("ETCD_SSL_CA"))
	viper.SetDefault("etcd.ssl.cert", os.Getenv("ETCD_SSL_CERT"))
	viper.SetDefault("etcd.ssl.key", os.Getenv("ETCD_SSL_KEY"))
	viper.SetDefault("etcd.ssl.verify", os.Getenv("ETCD_SSL_VERIFY"))
	// google cloud
	viper.SetDefault("gcp.credentials_file", os.Getenv("GOOGLE_APP_CREDENTIALS"))
	viper.SetDefault("gcp.bucket", os.Getenv("GCS_BUCKET"))
	// amazon services
	viper.SetDefault("aws.access_key", os.Getenv("AWS_ACCESS_KEY_ID"))
	viper.SetDefault("aws.secret_key", os.Getenv("AWS_SECRET_ACCESS_KEY"))
	viper.SetDefault("aws.region", os.Getenv("AWS_REGION"))
	viper.SetDefault("aws.bucket", os.Getenv("AWS_S3_BUCKET"))

}

func addOutputFormatFlag(cmd *cobra.Command) {
	cmd.Flags().StringP("output", "o", "yaml", "specify output format (yaml/json)")
}

func writeOutput(cmd *cobra.Command, v interface{}) error {
	outputFormat, _ := cmd.Flags().GetString("output")
	if outputFormat == "json" {
		return response.Json(os.Stdout, v)
	} else {
		return response.Yaml(os.Stdout, v)
	}
}

func handleOsSignal(fn func(sig os.Signal)) {
	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, os.Interrupt, os.Kill, syscall.SIGTERM)

	go func() {
		sig := <-exitChan
		fn(sig)
	}()
}
