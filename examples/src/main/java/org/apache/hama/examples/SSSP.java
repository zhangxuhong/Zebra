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
import java.util.Iterator;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.Combiner;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.MetisPartitioner;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

public class SSSP {

  public static final String START_VERTEX = "shortest.paths.start.vertex.name";

  public static class ShortestPathVertex extends
      Vertex<Text, IntWritable, IntWritable> {

    private int partialResult;
    private boolean partial = false;

    public ShortestPathVertex() {
      this.setValue(new IntWritable(Integer.MAX_VALUE));
    }

    public boolean isStartVertex() {
      Text startVertex = new Text(getConf().get(START_VERTEX));
      return (this.getVertexID().equals(startVertex)) ? true : false;
    }

    public void partialCompute(Iterator<IntWritable> messages)throws IOException{
      partialResult = isStartVertex() ? 0 : Integer.MAX_VALUE;
      if (messages.hasNext() && this.getSuperstepCount() != 1) {
        partial = true;
        IntWritable msg = messages.next();
        if (msg.get() < partialResult) {
          partialResult = msg.get();
        }
        while (messages.hasNext()) {
          if (msg.get() < partialResult) {
            partialResult = msg.get();
          }
        }   
      }
      
      //partial = true;

    }

    @Override
    public void compute(Iterator<IntWritable> messages) throws IOException {
      int minDist = isStartVertex() ? 0 : Integer.MAX_VALUE;

      if (this.getSuperstepCount() == 0){
        sendMessageToNeighbors(new IntWritable(Integer.MAX_VALUE));
        return;
      }
      if(this.getSuperstepCount() == 1){
        List<IntWritable> singletonList = Collections.singletonList(this.getValue());
        messages = singletonList.iterator();
      }
      if(partial){
          //partialResult = partialResult - sum;
        if (partialResult < minDist) {
          minDist = partialResult;
        }
        partial = false;
      }


      while (messages.hasNext()) {
        IntWritable msg = messages.next();
        if (msg.get() < minDist) {
          minDist = msg.get();
        }
      }

      if (minDist < this.getValue().get()) {
        this.setValue(new IntWritable(minDist));
        for (Edge<Text, IntWritable> e : this.getEdges()) {
          sendMessage(e, new IntWritable(minDist + e.getValue().get()));
        }
      } else {
        voteToHalt();
      }
    }
  }

  public static class MinIntCombiner extends Combiner<IntWritable> {

    @Override
    public IntWritable combine(Iterable<IntWritable> messages) {
      int minDist = Integer.MAX_VALUE;

      Iterator<IntWritable> it = messages.iterator();
      while (it.hasNext()) {
        int msgValue = it.next().get();
        if (minDist > msgValue)
          minDist = msgValue;
      }

      return new IntWritable(minDist);
    }
  }

  public static class SSSPTextReader extends
      VertexInputReader<LongWritable, Text, Text, IntWritable, IntWritable> {

    /**
     * The text file essentially should look like: <br/>
     * VERTEX_ID\t(n-tab separated VERTEX_ID:VERTEX_VALUE pairs)<br/>
     * E.G:<br/>
     * 1\t2:25\t3:32\t4:21<br/>
     * 2\t3:222\t1:922<br/>
     * etc.
     */
    @Override
    public boolean parseVertex(LongWritable key, Text value,
        Vertex<Text, IntWritable, IntWritable> vertex) throws Exception {
      String[] split = value.toString().split("\t");
      for (int i = 0; i < split.length; i++) {
        if (i == 0) {
          vertex.setVertexID(new Text(split[i]));
        } else {
          String[] split2 = split[i].split(":");
          vertex.addEdge(new Edge<Text, IntWritable>(new Text(split2[0]),
              new IntWritable(Integer.parseInt(split2[1]))));
        }
      }
      return true;
    }

  }

  private static void printUsage() {
    System.out.println("Usage: <startnode> <input> <output> [tasks]");
    System.exit(-1);
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    if (args.length < 3)
      printUsage();

    // Graph job configuration
    HamaConfiguration conf = new HamaConfiguration();
    GraphJob ssspJob = new GraphJob(conf, SSSP.class);
    // Set the job name
    ssspJob.setJobName("Single Source Shortest Path");

    conf.set(START_VERTEX, args[0]);
    ssspJob.setInputPath(new Path(args[1]));
    ssspJob.setOutputPath(new Path(args[2]));
    ssspJob.setPartitioner(HashPartitioner.class);
    if (args.length >= 4) {
      ssspJob.setNumBspTask(Integer.parseInt(args[3]));
    }
    if (args.length >= 5){
      if (args[4].equals("1"))
        ssspJob.setPartitioner(MetisPartitioner.class);
    }

    ssspJob.setVertexClass(ShortestPathVertex.class);
    ssspJob.setCombinerClass(MinIntCombiner.class);
    ssspJob.setInputFormat(TextInputFormat.class);
    ssspJob.setInputKeyClass(LongWritable.class);
    ssspJob.setInputValueClass(Text.class);

    
    ssspJob.setOutputFormat(TextOutputFormat.class);
    ssspJob.setVertexInputReaderClass(SSSPTextReader.class);
    ssspJob.setOutputKeyClass(Text.class);
    ssspJob.setOutputValueClass(IntWritable.class);
    // Iterate until all the nodes have been reached.
    ssspJob.setMaxIteration(Integer.MAX_VALUE);

    ssspJob.setVertexIDClass(Text.class);
    ssspJob.setVertexValueClass(IntWritable.class);
    ssspJob.setEdgeValueClass(IntWritable.class);

    long startTime = System.currentTimeMillis();
    if (ssspJob.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }
  }
}
