/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package me.nordiccloudteam;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.text.SimpleDateFormat;

import org.json.simple.JSONArray;
import org.json.simple.JSONValue;
import org.json.simple.JSONObject;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;

import me.nordiccloudteam.ImportCitybikesDataOptions;

/**
 */

public class ImportCitybikesData {

    static class UnpackJSONDataFn extends DoFn<KV<String, String>, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<String, String> item = c.element();
            String timestamp = item.getKey().replaceAll("\\D+","");
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.00");
            
            try {
                Object obj = JSONValue.parse(item.getValue());
                JSONObject jsonObject = (JSONObject)obj;
                JSONObject data = (JSONObject)jsonObject.get("data");
                JSONArray bikeRentalStations = (JSONArray)data.get("bikeRentalStations");
        
                bikeRentalStations.forEach((station) -> {
			        JSONObject info = (JSONObject)station;
                    TableRow row = new TableRow();
                    
                    for (Map.Entry<String, Object> entry : ((HashMap<String, Object>)info).entrySet()) {
                        String key = entry.getKey();
                        Object value = entry.getValue();
                        row.set(key, value);
                    }
                    row.set("timestamp", df.format(new java.util.Date(Integer.valueOf(timestamp) * 1000L)));
                    c.output(row);
		        });
            } catch (Exception e) {
                System.out.println("Failed to process file: " + item.getKey() + ": " + e.toString());
            }
        }
    }

    static class CitybikesDataTransform extends PTransform<PCollection<KV<String, String>>, PCollection<TableRow>> {

        @Override
        public PCollection<TableRow> expand(PCollection<KV<String, String>> data) {
            PCollection<TableRow> results = data.apply(ParDo.of(new UnpackJSONDataFn()));
            return results;
        }
    }

    public static void main(String[] args) {
        ImportCitybikesDataOptions options =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(ImportCitybikesDataOptions.class);

        runImportCitybikesData(options);
    }

    public static void runImportCitybikesData(ImportCitybikesDataOptions options) {
        Pipeline p = Pipeline.create(options);
    
        PCollection<TableRow> rows = 
            p.apply(FileIO.match().filepattern(options.getInput()))
            .apply(FileIO.readMatches())  
            .apply(ParDo.of(new DoFn<ReadableFile, KV<String, String>>() {
            @ProcessElement
            public void process(ProcessContext c) {
            ReadableFile f = c.element();
            String filename = f.getMetadata().resourceId().toString();
            try {
                String contents = f.readFullyAsUTF8String();
                c.output(KV.of(filename, contents));
            } catch (IOException e) {
                System.out.println("Received IOException: " + e.toString());
            }
        }
    })).apply(new CitybikesDataTransform());

    // Create schema
     List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("stationId").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("name").setType("STRING"));
    fields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
    fields.add(new TableFieldSchema().setName("lon").setType("FLOAT"));
    fields.add(new TableFieldSchema().setName("lat").setType("FLOAT"));
    fields.add(new TableFieldSchema().setName("spacesAvailable").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("bikesAvailable").setType("INTEGER"));
    TableSchema schema = new TableSchema().setFields(fields);

    // Write to BigQuery
    rows.apply(BigQueryIO
        .writeTableRows()
        .to(options.getOutput())
        .withSchema(schema)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

    p.run().waitUntilFinish();
  }
}
