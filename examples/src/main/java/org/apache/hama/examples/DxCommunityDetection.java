/**
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

package org.apache.hama.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.MetisPartitioner;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

public class DxCommunityDetection {

  private static final Log LOG = LogFactory.getLog(DxCommunityDetection.class);

  public static class DxCommunityDetectionVertex extends
      Vertex<Text, NullWritable, Text> {
    Text lastLabel = null;

    private boolean partial = false;
    private Long partialmaxCount = null;
    private Text partialmaxLabel = null;
    private HashMap<Text, Long> partiallabelCounts = null;

    public void partialCompute(Iterator<Text> msgs) throws IOException{
      if (this.getEdges().size() == 0) {
        return;
      }
      if(msgs.hasNext()){
        partial = true;
        partialmaxCount = new Long(0);
        partialmaxLabel = null;
        Text label = null;
        partiallabelCounts = new HashMap<Text, Long>();
        label = msgs.next();
        Long labelCount = partiallabelCounts.get(label);
        if (labelCount == null) {
          labelCount = new Long(1);
          partiallabelCounts.put(label, labelCount);
        } else {
          labelCount = new Long(labelCount.longValue() + 1);
          partiallabelCounts.put(label, labelCount);
        }
        if (labelCount.longValue() > partialmaxCount.longValue()) {
          partialmaxLabel = label;
          partialmaxCount = labelCount;
        } else if (labelCount.longValue() == partialmaxCount.longValue()){
          int max = Integer.parseInt(partialmaxLabel.toString());
          int l = Integer.parseInt(label.toString());
          if (l > max){
            partialmaxLabel = label;
          }
        }

        while(msgs.hasNext()){
          label = msgs.next();
          labelCount = partiallabelCounts.get(label);
          if (labelCount == null) {
            labelCount = new Long(1);
            partiallabelCounts.put(label, labelCount);
          } else {
            labelCount = new Long(labelCount.longValue() + 1);
            partiallabelCounts.put(label, labelCount);
          }
          if (labelCount.longValue() > partialmaxCount.longValue()) {
            partialmaxLabel = label;
            partialmaxCount = labelCount;
          } else if (labelCount.longValue() == partialmaxCount.longValue()){
            int max = Integer.parseInt(partialmaxLabel.toString());
            int l = Integer.parseInt(label.toString());
            if (l > max){
              partialmaxLabel = label;
            }
          }
        }
      }
      

    }

    @Override
    public void compute(Iterator<Text> msgs) throws IOException {

      Long maxCount = new Long(0);
      Text maxLabel = null;
      Text label = null;
      HashMap<Text, Long> labelCounts = new HashMap<Text, Long>();

      if(partial){
        maxCount = partialmaxCount;
        maxLabel = partialmaxLabel;
        labelCounts = partiallabelCounts;
        partiallabelCounts = null;
        partial = false;
      }

      long supersteps = this.getSuperstepCount();
      if (supersteps == 0) {
        this.setValue(this.getVertexID());
        lastLabel = this.getVertexID();
      } else {
        if (this.getEdges().size() == 0) {
          return;
        }
        while (msgs.hasNext()) {
          // label = Long.parseLong(msgs.next().toString());
          label = msgs.next();
          Long labelCount = labelCounts.get(label);
          if (labelCount == null) {
            labelCount = new Long(1);
            labelCounts.put(label, labelCount);
          } else {
            labelCount = new Long(labelCount.longValue() + 1);
            labelCounts.put(label, labelCount);
          }
          if (labelCount.longValue() > maxCount.longValue()) {
            maxLabel = label;
            maxCount = labelCount;
          } else if (labelCount.longValue() == maxCount.longValue()){
            int max = Integer.parseInt(maxLabel.toString());
            int l = Integer.parseInt(label.toString());
            if (l > max){
              maxLabel = label;
            }
          }
        }
        labelCounts = null;
        if (maxLabel != null) {
          this.setValue(maxLabel);
        }
      }
      this.sendMessageToNeighbors(this.getValue());
    } // end of compute()
  } // end of class DxCommunityDetectionVertex

  public static class CommunityDetectionTextReader extends
      VertexInputReader<LongWritable, Text, Text, NullWritable, DoubleWritable> {

    String lastVertexId = null;
    List<String> adjacents = new ArrayList<String>();

    @Override
    public boolean parseVertex(LongWritable key, Text value,
        Vertex<Text, NullWritable, DoubleWritable> vertex) {
      // the tail vertex of input
      if (key == null || value == null) {
        assert (lastVertexId != null);
        vertex.setVertexID(new Text(lastVertexId));
        for (String adjacent : adjacents) {
          vertex
              .addEdge(new Edge<Text, NullWritable>(new Text(adjacent), null));
        }
        adjacents.clear();
        lastVertexId = null;
        return true;
      }

      // normal parse
      String line = value.toString();
      String[] lineSplit = line.split(" ");
      if (!line.startsWith("#")) {
        if (lastVertexId == null) {
          lastVertexId = lineSplit[0];
        }
        if (lastVertexId.equals(lineSplit[0])) {
          adjacents.add(lineSplit[1]);
        } else {
          vertex.setVertexID(new Text(lastVertexId));
          for (String adjacent : adjacents) {
            vertex.addEdge(new Edge<Text, NullWritable>(new Text(adjacent),
                null));
          }
          adjacents.clear();
          lastVertexId = lineSplit[0];
          adjacents.add(lineSplit[1]);
          return true;
        }
      }
      return false;
    }
  }

  public static class DxCommunityDetectionTextReader extends
      VertexInputReader<LongWritable, Text, Text, NullWritable, Text> {

    String lastVertexId = null;
    List<String> adjacents = new ArrayList<String>();

    // LongWritable is the key type and Text is the whole line including vertexID and vertex value
    @Override
    public boolean parseVertex(LongWritable key, Text value,
        Vertex<Text, NullWritable, Text> vertex) throws Exception {
      String line = value.toString();
      String[] splits = line.split("\t");
      for (int i = 0; i < splits.length; i++) {
        if (i == 0) {
          vertex.setVertexID(new Text(splits[0]));
        } else {
          vertex
              .addEdge(new Edge<Text, NullWritable>(new Text(splits[i]), null));
        }
      }
      return true;
    } // end of parseVertex

    public boolean dxParseVertex(LongWritable key, Text value,
        Vertex<Text, NullWritable, Text> vertex,
        Vertex<Text, NullWritable, Text> vertexNext) throws Exception {
      String line = value.toString();
      String[] lineSplit = line.split(" ");

      Assert.assertNotNull(vertex.getEdges());

      if (!line.startsWith("#")) {
        if (lastVertexId == null) {
          lastVertexId = lineSplit[0];
          vertex.setVertexID(new Text(lastVertexId));
        }
        if (vertexNext.getEdges().size() != 0) {
          Assert.assertNotNull(vertexNext.getVertexID());
          if (vertex.getVertexID() == null)
            vertex.setVertexID(vertexNext.getVertexID());
          vertex.addEdge(vertexNext.getEdges().iterator().next());
          vertexNext.setVertexID(null);
          vertexNext.getEdges().clear();
        }
        if (lastVertexId.equals(lineSplit[0])) {
          vertex.addEdge(new Edge<Text, NullWritable>(new Text(lineSplit[1]),
              null));
        } else {
          lastVertexId = lineSplit[0];
          vertexNext.setVertexID(new Text(lastVertexId));
          vertexNext.addEdge(new Edge<Text, NullWritable>(
              new Text(lineSplit[1]), null));
          return true;
        }
      }
      return false;
    }

  } // end of DxCommunityDetectionTextReader

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    HamaConfiguration conf = new HamaConfiguration(new Configuration());
    GraphJob cdJob = createJob(args, conf);
    long startTime = System.currentTimeMillis();
    if (cdJob.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }
  } // end of main()

  public static GraphJob createJob(String[] args, HamaConfiguration conf)
      throws IOException {
    GraphJob cdJob = new GraphJob(conf, DxCommunityDetection.class);
    cdJob.setJobName("DxCommunityDetection");

    cdJob.setVertexClass(DxCommunityDetectionVertex.class);
    cdJob.setInputPath(new Path(args[0]));
    cdJob.setOutputPath(new Path(args[1]));

    // set the defaults
    cdJob.setMaxIteration(30);
    cdJob.setNumBspTask(3);
    cdJob.setPartitioner(HashPartitioner.class);

    if (args.length >= 3)
      cdJob.setNumBspTask(Integer.parseInt(args[2]));
    if (args.length >= 4)
      cdJob.setMaxIteration(Integer.parseInt(args[3]));
    if (args.length >= 5){
      if (args[4].equals("1"))
        cdJob.setPartitioner(MetisPartitioner.class);
    }
 

    cdJob.setVertexIDClass(Text.class);
    cdJob.setVertexValueClass(Text.class);
    cdJob.setEdgeValueClass(NullWritable.class);

    cdJob.setInputKeyClass(LongWritable.class);
    cdJob.setInputValueClass(Text.class);
    cdJob.setInputFormat(TextInputFormat.class);
    cdJob.setVertexInputReaderClass(DxCommunityDetectionTextReader.class);
    cdJob.setOutputFormat(TextOutputFormat.class);
    cdJob.setOutputKeyClass(Text.class);
    cdJob.setOutputValueClass(Text.class);
    return cdJob;
  } // end of createJob()

} // end of DxCommunityDetection
