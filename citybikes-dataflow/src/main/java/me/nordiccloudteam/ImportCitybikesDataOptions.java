package me.nordiccloudteam;

import org.apache.beam.sdk.annotations.*;
import org.apache.beam.sdk.options.PipelineOptions;

public interface ImportCitybikesDataOptions extends PipelineOptions {

    /**
     * By default, this example reads from a public dataset containing the text of King Lear. Set
     * this option to choose a different input file or glob.
     */
    //@Description("Path of the file to read from")
    //@Default.String("gs://citybikes/data/*.json")
    String getInput();

    void setInput(String value);

    /** Set this required option to specify where to write the output. */
    //@Description("BQ table to write to")
    //@Required
    String getOutput();

    void setOutput(String value);
}