package cmd

import (
	"context"
	"fmt"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/authzed/zed/internal/client"
)

// this command is not fully tested, use at your own risk
func registerDestroyCmd(rootCmd *cobra.Command) {
	rootCmd.AddCommand(destroyCmd)
	destroyCmd.Flags().Int("batch-size", 1_000, "destroy relationship write batch size")
	destroyCmd.Flags().Int("parallelism", 1, "number of definitions to be deleted in parallel")
	destroyCmd.Flags().Bool("continue-on-failure", false, "ignores failures and continues destroying with the next batch")
}

var destroyCmd = &cobra.Command{
	Use:   "destroy <filename>",
	Short: "Destroy a permission system's relationships'",
	Args:  cobra.MaximumNArgs(0),
	RunE:  destroyCmdFunc,
}

type destroyResult struct {
	namespace      string
	err            error
	batchesWritten uint64
}

func startWorker(ctx context.Context, client client.Client, inputs <-chan string, results chan<- destroyResult, cmd *cobra.Command) {
	go func() {
		batchSize := uint32(cobrautil.MustGetInt(cmd, "batch-size"))
		continueOnFailure := cobrautil.MustGetBool(cmd, "continue-on-failure")

		for namespace := range inputs {
			var batchesWritten uint64
			delProgress := v1.DeleteRelationshipsResponse_DELETION_PROGRESS_PARTIAL
			for delProgress != *v1.DeleteRelationshipsResponse_DELETION_PROGRESS_COMPLETE.Enum() {
				resp, err := client.DeleteRelationships(ctx, &v1.DeleteRelationshipsRequest{
					RelationshipFilter: &v1.RelationshipFilter{
						ResourceType: namespace,
					},
					OptionalLimit:                 batchSize,
					OptionalAllowPartialDeletions: true,
				})
				batchesWritten++
				if err != nil {
					if continueOnFailure {
						log.Error().Err(err).Msgf("error deleting relationships for definiton %s, but continuing", namespace)
					} else {
						results <- destroyResult{
							namespace:      namespace,
							err:            fmt.Errorf("error deleting relationships for definiton %s: %w", namespace, err),
							batchesWritten: batchesWritten,
						}
						return
					}
				}
				if err == nil {
					delProgress = resp.DeletionProgress
				}
			}
			results <- destroyResult{
				namespace:      namespace,
				err:            nil,
				batchesWritten: batchesWritten,
			}
		}
	}()
}

func destroyCmdFunc(cmd *cobra.Command, args []string) error {
	client, err := client.NewClient(cmd)
	if err != nil {
		return fmt.Errorf("unable to initialize client: %w", err)
	}

	ctx := cmd.Context()

	schemaResp, err := client.ReadSchema(ctx, &v1.ReadSchemaRequest{})
	if err != nil {
		return fmt.Errorf("error reading schema: %w", err)
	}

	namespaces, err := getNamespaces(schemaResp.SchemaText)
	if err != nil {
		return fmt.Errorf("error loading definitions from schema: %w", err)
	}

	relationshipDeleteStart := time.Now()
	var batchesWritten uint64

	inputs := make(chan string)
	results := make(chan destroyResult)

	parallelism := cobrautil.MustGetInt(cmd, "parallelism")
	for i := 0; i < parallelism; i++ {
		go func() {
			startWorker(ctx, client, inputs, results, cmd)
		}()
	}

	go func() {
		for _, namespace := range namespaces {
			inputs <- namespace
		}
	}()

	batchSize := uint32(cobrautil.MustGetInt(cmd, "batch-size"))
	resultsReceived := 0
	for result := range results {
		resultsReceived++
		if result.err != nil {
			return result.err
		}
		batchesWritten += result.batchesWritten
		log.Info().Msgf("deleted all relationships for definition %s - approximately %d relationships (%d/%d relations done)", result.namespace, batchSize*uint32(result.batchesWritten), resultsReceived, len(namespaces))
		if resultsReceived == len(namespaces) {
			close(inputs)
			close(results)
		}
	}

	totalTime := time.Since(relationshipDeleteStart)
	batchesPerSec := float64(batchesWritten) / totalTime.Seconds()

	log.Info().
		Uint64("batches", batchesWritten).
		Stringer("duration", totalTime).
		Float64("batchesPerSecond", batchesPerSec).
		Msg("finished destroy")

	return nil
}
func getNamespaces(schema string) ([]string, error) {
	emptyPrefix := ""
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source: input.Source("schema"), SchemaString: schema,
	}, &emptyPrefix)
	if err != nil {
		return []string{}, err
	}
	namespaces := make([]string, len(compiled.ObjectDefinitions))
	for i, def := range compiled.ObjectDefinitions {
		namespaces[i] = def.GetName()
	}

	return namespaces, err
}
