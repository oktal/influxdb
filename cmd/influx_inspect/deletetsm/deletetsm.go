// Package deletetsm bulk deletes a measurement from a raw tsm file.
package deletetsm

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"path/filepath"
	"strings"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// Command represents the program execution for "influxd deletetsm".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	dataDir           string // Path to the data directory
	database          string // Optional database
	retentionPolicy   string // Optional retention policy
	seriesFile        string // Path to the files of series to delete
	sanitize          bool   // remove all keys with non-printable unicode
	verbose           bool   // verbose logging

	tsmFiles map[string][]string
	series   map[string]bool
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
		tsmFiles: make(map[string][]string),
		series: make(map[string]bool),
	}
}

// Run executes the command.
func (cmd *Command) Run(args ...string) (err error) {
	fs := flag.NewFlagSet("deletetsm", flag.ExitOnError)
	fs.StringVar(&cmd.dataDir, "dataDir", "/var/lib/influxdb/data", "Data storage path")
	fs.StringVar(&cmd.database, "database", "", "The optional database")
	fs.StringVar(&cmd.retentionPolicy, "retention", "", "The optional retention policy")
	fs.StringVar(&cmd.seriesFile, "seriesFile", "", "The path to the files of series do delete")
	fs.BoolVar(&cmd.sanitize, "sanitize", false, "")
	fs.BoolVar(&cmd.verbose, "v", false, "")
	fs.SetOutput(cmd.Stdout)
	fs.Usage = cmd.printUsage
	if err := fs.Parse(args); err != nil {
		return err
	}

	if !cmd.verbose {
		log.SetOutput(ioutil.Discard)
	}

	if cmd.seriesFile == "" {
		return fmt.Errorf("-seriesFile flag required")
	}

	if err := cmd.walkTSMFiles(); err != nil {
		return err
	}

	if err := cmd.parseSeriesFile(); err != nil {
		return err
	}

	for k, _ := range cmd.series {
		fmt.Println(k)
	}

	if err := cmd.deleteSeries(); err != nil {
		return err
	}

	return nil
}

func (cmd *Command) walkTSMFiles() error {
	return filepath.Walk(cmd.dataDir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// check to see if this is a tsm file
		if filepath.Ext(path) != "."+tsm1.TSMFileExtension {
			return nil
		}

		relPath, err := filepath.Rel(cmd.dataDir, path)
		if err != nil {
			return err
		}
		dirs := strings.Split(relPath, string(byte(os.PathSeparator)))
		if len(dirs) < 2 {
			return fmt.Errorf("invalid directory structure for %s", path)
		}
		if dirs[0] == cmd.database || cmd.database == "" {
			if dirs[1] == cmd.retentionPolicy || cmd.retentionPolicy == "" {
				key := filepath.Join(dirs[0], dirs[1])
				cmd.tsmFiles[key] = append(cmd.tsmFiles[key], path)
			}
		}
		return nil
	})
}

func (cmd* Command) parseSeriesFile() error {
	file, err := os.Open(cmd.seriesFile)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		cmd.series[scanner.Text()] = true;
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

func (cmd *Command) deleteSeries() error {
	for key := range cmd.tsmFiles {
		if files, ok := cmd.tsmFiles[key]; ok {
			fmt.Fprintf(cmd.Stdout, "Processing TSM files for '%s'\n", key)	
			if err := cmd.deleteFromTSMFiles(files); err != nil {
				return err
			}
		}
	}

	return nil
}

func (cmd* Command) deleteFromTSMFiles(files []string) error {
	// we need to make sure we write the same order that the files were written
	sort.Strings(files)

	for _, file := range files {
		fmt.Fprintf(cmd.Stdout, "Processing data for TSM file '%s'\n", file)
		cmd.processTSMFile(file)
	}

	return nil
}


func (cmd *Command) processTSMFile(path string) error {
	// Open TSM reader.
	input, err := os.Open(path)
	if err != nil {
		return err
	}
	defer input.Close()

	r, err := tsm1.NewTSMReader(input)
	if err != nil {
		return fmt.Errorf("unable to read %s: %s", path, err)
	}
	defer r.Close()

	// Remove previous temporary files.
	outputPath := path + ".rewriting.tmp"
	if err := os.RemoveAll(outputPath); err != nil {
		return err
	} else if err := os.RemoveAll(outputPath + ".idx.tmp"); err != nil {
		return err
	}

	log.Printf("Creating temporary file '%s'", outputPath)

	// Create TSMWriter to temporary location.
	output, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer output.Close()

	w, err := tsm1.NewTSMWriter(output)
	if err != nil {
		return err
	}
	defer w.Close()

	// Iterate over the input blocks.
	itr := r.BlockIterator()
	droppedBlocksCount := 0
	for itr.Next() {
		// Read key & time range.
		key, minTime, maxTime, _, _, block, err := itr.Read()
		if err != nil {
			return err
		}

		// Skip block if this is the measurement and time range we are deleting.
		seriesBytes, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
		serieKey, err := cmd.formatSerieKey(seriesBytes)
		if err != nil {
			return err
		}
		if _, ok := cmd.series[serieKey]; ok {
			log.Printf("deleting block: %s (%s-%s) sz=%d\n",
				key,
				time.Unix(0, minTime).UTC().Format(time.RFC3339Nano),
				time.Unix(0, maxTime).UTC().Format(time.RFC3339Nano),
				len(block),
			)
			droppedBlocksCount += 1
			continue
		}

		if err := w.WriteBlock(key, minTime, maxTime, block); err != nil {
			return err
		}
	}

	fmt.Fprintf(cmd.Stdout, "Dropped '%d' total blocks\n", droppedBlocksCount)

	// Write index & close.
	if err := w.WriteIndex(); err != nil {
		return err
	} else if err := w.Close(); err != nil {
		return err
	}

	// Replace original file with new file.
	return os.Rename(outputPath, path)
}

func (cmd* Command) formatSerieKey(seriesBytes []byte) (string, error) {
	measurement, tags := models.ParseKey(seriesBytes)
	var b strings.Builder

	fmt.Fprintf(&b, "%s,", measurement);
	for idx, tag := range tags {
		fmt.Fprintf(&b, "%s=%s", tag.Key, tag.Value)
		if (idx < len(tags) - 1) {
			b.WriteByte(',')
		}
	}

	return b.String(), nil
}

func (cmd *Command) printUsage() {
	fmt.Print(`Deletes a measurement from a raw tsm file.

Usage: influx_inspect deletetsm [flags] path...

    -dataDir PATH
		   Data storage Path
	-database database
			Optional database
	-retention retentionPolicy
			Optional retention policy
    -sanitize
            Remove all keys with non-printable unicode characters.
    -v
            Enable verbose logging.`)
}
